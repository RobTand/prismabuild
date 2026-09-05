# SLURM runbook, 2026-09-04

This runbook installs SLURM across the three fleet boxes and cuts `pbrun` over
to it. It is four scripts and the order to run them in:

| Script | Who runs it | What it does |
|---|---|---|
| `fleet/slurm/install.sh` | root, on each box | Phase 1: the user, munge, the packages, the configuration, the daemons |
| `fleet/slurm/verify.sh` | rob, from any box | the validation checks, PASS or FAIL per row |
| `fleet/slurm/cutover.sh` | rob, from a checkout | Phase 2: stop the pull queue's plane, publish SLURM as the default |
| `fleet/slurm/rollback.sh` | rob, from a checkout | reverse the cutover |

Every one of them takes `--dry-run` except `verify.sh`, which changes nothing.
A dry run prints the commands verbatim and runs none of them, so read one
before you run the real thing.

The code they configure is already merged and tested: `prismabuild.slurm_lane`
(submit, wait, cancel), `tools/fleet/slurm_job.py` (what a batch job runs), and
`pbrun --transport slurm`. The pull queue is untouched and remains the default
until `cutover.sh` publishes a generation that says otherwise.

## Verified in a container, 2026-09-04

SLURM is still installed on no box in this fleet, but the lane is no longer
untested against a real scheduler. `fleet/slurm/smoke/` brings up one
`slurmctld` and one `slurmd` in a privileged container -- the real `epilog.sh`,
the fleet's scheduler choices, a two-shard node -- and drives eleven named rows
through it.

```bash
DEB_DIR=/home/rob/slurm-build/arm64-24.04 fleet/slurm/smoke/run.sh
PRISMABUILD_SLURM_SMOKE=1 PYTHONPATH=src pytest -q tests/test_slurm_smoke.py
```

Both SLURMs pass all eleven rows: the fleet's 25.11.2 rebuild (primary) and
Ubuntu 24.04's own 23.11.4 (secondary). What that settles:

- **The `shard` syntax in `gres.conf`** (was item 1). `File=` is *required*, not
  optional: a `shard` with no sharing `gpu` bound to a device file is fatal at
  `slurmd` start (`SHARING configuration lacks "File" specification` /
  `failed to merge SHARED and SHARING configuration`). The fleet's
  `Name=gpu File=/dev/nvidia0` plus `Name=shard Count=N File=/dev/nvidia0` is
  the form that works, and `--gres=shard:1` schedules against it: two jobs run
  concurrently on a two-shard node and a third waits on `Resources`.
- **The cgroup attestation the worker performs** (was item 2). With
  `ProctrackType=proctrack/cgroup` a job's processes land in a `job_<id>`
  cgroup and `core._verify_slurm_process_membership` accepts it; actions
  execute and publish receipts. This is not optional -- a fallback to
  `proctrack/linuxproc` fails *every* action inside the worker, so the smoke
  refuses to run its rows without cgroup containment rather than reporting a
  softer result.
- **`--export=NIL` and Git** (was item 3). A batch job with no `HOME` and no
  `PATH` materializes the sealed snapshot and runs the worker without
  complaint. No change to `job_script_text` is needed.
- **The Epilog over NFS** (was item 7). Answered by measurement rather than by
  the smoke: dl380g10's `/etc/exports` reads
  `/storage_pool/shared 192.168.1.180(rw,async,no_subtree_check) 192.168.1.110(rw,async,no_subtree_check)`
  -- no `no_root_squash` -- so the compute node's `root` is `nobody` there and
  its unlink of `jobs/<id>.job` fails silently. `epilog.sh` now performs every
  delete below the lane root as `runuser -u "$SLURM_JOB_USER"`, covered by
  `tests/test_slurm_epilog.py`, and logs a line if a state file survives
  anyway. Smoke row 8 reads that delete back out of `slurmd.log`
  (`removed state file ... as rob`), so the squash-safe path is known to be the
  one that runs -- on a bind mount, which is the part NFS still has to confirm.
  Nothing to do at install: `verify.sh` row 8 reads `jobs/` back out.
- **`CPUs=` for the two GB10 boxes** (was item 5). Measured and written into
  `slurm.conf`: 20 CPUs as one socket of twenty, one thread per core, measured
  again on 2026-09-05 with `slurmd -C` from the fleet's own 25.11.2 build.
  `install.sh` cross-checks that against each box's stanza and refuses on a
  mismatch, so the box is the authority and the file is the claim.
- **`--timeout-s` enforcement, withdrawal, and the terminal records.** Also
  covered: `--timeout-s` becomes `--time` and SLURM kills the job (arriving as
  `detail.slurm.state=TIMEOUT`, not as a `pbrun` timeout); `--withdraw` on a
  running job `scancel`s it and files both the marker and a `failed/` record;
  and `done/`/`failed/` records carry the job id, the node and the exit status
  the pull queue's readers expect.

Two lane defects the fakes could not see were found and fixed here:

- A resubmission of one action key collided with its own sealed submission
  record and was refused at submit, which put the CAS hit, a re-run of a failed
  action and `pool_reset` all out of reach.  Submission records are now named
  `<published_unix>-<attempt>.json`.
- A job SLURM killed at the time limit arrives as `ExitCode=0:15` -- exit code
  zero, signal fifteen -- so `pbrun` announced that it "exited 0 but published
  no receipt" and pointed the operator at an empty job log instead of at
  `TIMEOUT`.  It reads `Outcome.succeeded` now.

And one thing an operator would have assumed wrongly: the Epilog's environment
is SLURM's own, built from its `SLURM_*` variables, so nothing a submitter
exports reaches it. `PRISMABUILD_EPILOG_DOCKER` and
`PRISMABUILD_SLURM_LANE_ROOT` are test-only levers, not install-time settings,
and in particular do not set `PRISMABUILD_SLURM_LANE_ROOT` on a submitter
expecting the Epilog to honour it: the Epilog reads its job-state root from its
own hardcoded default and cannot see that variable, so the two would point at
different directories and the state file would never be cleaned up. Another
worker is making the node-side path independent of the submitter's
environment; until that lands, leave the lane root alone. The Epilog finds
`docker` on `PATH`.

## Still not verified

These need the real install, and the container cannot stand in for any of them:

1. **The cgroup *plugins* as the fleet will run them.** Delegation in the
   container needs `--privileged`, `--cgroupns=private`, a hand-written
   `cgroup.subtree_control` and `IgnoreSystemd=yes`. The fleet's boxes have
   systemd and `slurmd` under it, which is a different arrangement; all three
   boxes are cgroup v2 (`cgroup2fs`) with `cpuset cpu io memory hugetlb pids
   rdma misc dmem` available, measured 2026-09-04.
2. **Device containment and real GPUs.** `ConstrainDevices` is off in the
   container and the node's GRES binds a `mknod`'d character device nothing
   opens. On cgroup v2 the containment is an eBPF program that denies exactly
   the GRES `File=` devices a job was not allocated and admits everything else,
   so what has to be shown on a real box is that a job holding `shard:1` can
   still initialize CUDA while a job holding nothing cannot open
   `/dev/nvidia0`. `verify.sh` rows 3 and 4 are that pair. If a GPU job fails
   at CUDA init while a non-GPU job runs, the `File=` paths in `gres.conf` are
   the first place to look. Note also that the fleet's 25.11.2 build ships no
   `gpu_nvml.so`, so `AutoDetect=nvml` is not available at all and `gres.conf`
   must stay static.
3. **NFS `root_squash` end to end.** The export is measured and the Epilog is
   fixed, but the fix has been exercised only against a fake `runuser` and a
   local bind mount. The first killed job on the real fleet is the test.
4. **Three-box RPC.** One node cannot show a controller talking to a remote
   `slurmd`, a node draining and returning under `ReturnToService=2`, or an
   action landing on a box other than the submitter's.
5. **Whether the Sparks' rebuilt 25.11.2 interoperates with dl380g10's 25.11.2
   from apt** (was item 6). The Sparks' packages are a rebuild of Ubuntu
   26.04's own source package at the same patch version, and both ends have now
   been exercised separately; they have not been exercised against each other.
6. **The four scripts in this runbook.** None of them has run on the fleet,
   because SLURM is installed on no box. `install.sh` has been run for real
   through step 7 in an `ubuntu:24.04` arm64 container on sparky with a faked
   `hostname` -- the packages install, the topology cross-check passes, the
   munge key round-trips and is shredded, and a second run skips every
   completed step -- and step 8 is where a container stops, having no systemd.
   `verify.sh` has never run at all: every row of it needs a controller.
   `cutover.sh` and `rollback.sh` have been exercised only in `--dry-run` and
   through their refusal paths against a sandbox queue.
7. **That the addresses in `slurm.conf` are enough.** The resolution failure in
   both directions is measured and the addresses are measured, but no SLURM
   daemon has yet dialled one of them. `NodeAddr` is the documented remedy for
   exactly this; it has not been shown working on this fleet.

## What this replaces, and what it does not

`pool.py` and `tools/fleet/worker_loop.py` implement a pull queue over NFS:
workers claim sealed actions by `rename()`, hold a lease, and admit work against
a memory and GPU-slot budget. SLURM replaces the claiming, the leasing, and the
admission. It does not replace anything in `core.py`: action keys, the CAS,
receipts, attestation, and the git-bundle checkout snapshot are the same objects
under either transport, and `slurm_job.py` materializes a snapshot through the
same `prismabuild.materialize` code that a pull-queue worker runs.

It also does not replace the queue's *records*. Eleven fleet tools and
Tessera's `merge_suite.py` read one action's ending out of
`pb-queue/done/<key>.json` or `pb-queue/failed/<key>.json`, and `pool_reset`
reads `pb-queue/withdrawn/` before it re-submits anything. The pull queue writes
those from `PoolQueue.finish`, on the worker holding the claim. Under SLURM
there is no such worker, so the submitting `pbrun` writes them instead, into the
same three directories, under the schema id
`prismaquant.prismabuild.slurm_outcome.v1`. Readers that take these records by
field name keep working across the cutover with no change; a reader that wants
to tell a SLURM ending from a pull-queue one has the `schema` and `transport`
fields to do it with.

One gap in that arrangement is worth knowing before the cutover, because it is
structural rather than a defect. **The submitter is the writer.** A `pbrun`
killed mid-wait, or one whose `--wait-s` expired, files no terminal record for a
job that ends afterwards; `finish` runs on the box doing the work and cannot
miss it. The job's own `.out`, `.err` and submission record are all still under
`/mnt/shared/prismabuild-fleet/slurm/<action key>/`, and the CAS receipt is
still the authority on whether the work was done, so nothing is lost except the
summary. Closing it would mean writing the record from the job script's exit
trap, which cannot see the CAS verdict the record reports.

Two capabilities of the pull queue have no equivalent yet:

- **Aging.** `pool.py` counts denials in `passes` and lets a starved item
  withhold a host past `STARVATION_FLOOR`. Age-based priority in SLURM needs
  `priority/multifactor`, which needs `slurmdbd`, which this install defers.
  Until then the fleet runs FIFO within a priority, plus backfill.
- **`sacct`.** With `AccountingStorageType=accounting_storage/none`, `sacct`
  fails for every job. The lane tries it first anyway and falls back to
  `scontrol` and `squeue`, so deploying `slurmdbd` later is a configuration
  change and not a code change. Until it is deployed, `scontrol` answers the
  provenance the terminal record carries -- `claimed_host`, `elapsed_s`,
  the start and end times -- and only for as long as `MinJobAge` keeps the job.
  Past that the record files nulls in those fields rather than guesses.

## Fleet layout

| Box | Role | Architecture | SLURM source |
|---|---|---|---|
| dl380g10 | Controller and CPU node | x86_64, Ubuntu 26.04.1 | `apt install slurm-wlm` (25.11.2) |
| sparky | GPU node | aarch64, Ubuntu 24.04 | prebuilt debs at `/home/rob/slurm-build/arm64-24.04` (25.11.2) |
| gx10-6b77 (sparklina) | GPU node | aarch64, Ubuntu 24.04 | the same debs, already present on the box |

The Sparks cannot use Ubuntu 24.04's own `slurm-wlm`: the archive carries
23.11.4, and a 25.11 controller accepts nodes running 25.05, 24.11 or 24.05
only. **The packages they need are already built.** They are a rebuild of
Ubuntu 26.04's own 25.11.2 source package on 24.04 arm64, they are present on
both Sparks under `/home/rob/slurm-build/arm64-24.04`, and `SHA256SUMS` there
verifies clean. `/home/rob/slurm-build/BUILD.md` is the build record, including
the one packaging change it needed and what was and was not verified. There is
nothing to build during this install.

## Phase 1: install SLURM, one script per box

`fleet/slurm/install.sh` is steps 1 to 7 of the original runbook. It switches on
`hostname -s`, so the same script installs a controller on dl380g10 and a node
on each Spark, and a box it does not recognize is refused rather than guessed
at. It is idempotent: every step asks whether it is already done, says so, and
moves on.

Read a dry run first. It needs no root and changes nothing:

```bash
cd /home/rob/prismabuild
fleet/slurm/install.sh --dry-run
```

### Getting the scripts onto each box

Only sparky has a `prismabuild` checkout (measured 2026-09-05: dl380g10 and
sparklina do not), and `fleet/` is not among the files `publish_runtime`
mirrors to `/mnt/shared`, so the scripts do not arrive on their own. Copy the
one directory. `install.sh` reads its configuration files from beside itself,
so a flat copy is enough:

```bash
cd /home/rob/prismabuild
for box in dl380g10 sparklina; do
    ssh "$box" mkdir -p /home/rob/pb-slurm
    scp fleet/slurm/*.sh fleet/slurm/*.conf "$box":/home/rob/pb-slurm/
done
```

Copy again after any change to `slurm.conf`: `verify.sh` row 0b compares all
three boxes against the checkout and fails on drift.

Run every one of these from sparky. It is the only box that can reach the other
two by name.

### The names do not resolve, so the addresses are in the file

Measured 2026-09-05, and it is the reason `slurm.conf` carries
`SlurmctldHost=dl380g10(192.168.1.107)` and a `NodeAddr=` on every node:

- dl380g10 resolves neither Spark. `getent hosts sparky` and `getent hosts
  gx10-6b77` both return nothing, and `ssh sparky` from that box fails on the
  name. Its nsswitch is `files dns mymachines`, avahi is inactive, and neither
  Spark is in DNS or in its `/etc/hosts`. slurmctld would have had no address
  to contact a node on, and the message for that names a node, not a resolver.
- The Sparks resolve dl380g10, wrong answers first. `getent hosts dl380g10`
  gives `::`, and `getent ahostsv4 dl380g10` gives 192.168.1.165, a host that
  does not answer ping, before the live 192.168.1.107 on `bond0`. Both come
  from the router at 192.168.1.1, which serves the `.lan` zone and holds two A
  records for `dl380g10.lan`: `dig +short @192.168.1.1 dl380g10.lan A` returns
  .165 then .107. It is a stale DHCP record on the router, not avahi --
  `mdns4_minimal` answers `.local` only, and `dl380g10.local` times out. Ask
  whoever administers the router to delete the .165 record; a controller
  address that is right on the third try is a fleet that works intermittently.

Ports were measured open the same day: 6817 and 6818 answer "connection
refused" rather than timing out, in both directions, so `ufw` (active on all
three boxes) is not in the way and there is nothing to open.

The three addresses are DHCP leases rather than reservations, so they can move.
`install.sh` refuses to install a `slurm.conf` whose `NodeAddr` for this box is
not one of this box's own addresses, so a lease that moved is a refusal naming
the step rather than a node that quietly never registers. The same addresses
are already pinned in the NFS export `fleet/slurm/epilog.sh` records, so a move
breaks `/mnt/shared` before it breaks the scheduler.

### Order, and the one thing you carry between boxes

Run dl380g10 first. It creates the fleet's munge key and leaves a base64 copy
at `/home/rob/.munge-key.b64`, mode 0600, owned by rob. Every box must
authenticate with that same key.

```bash
# 1. on dl380g10, from the copy you made above
ssh -t dl380g10 sudo bash /home/rob/pb-slurm/install.sh

# 2. as rob, from sparky, pull the key and hand it on.  dl380g10 cannot push
#    it: that box resolves neither Spark, so ssh from there fails on the name.
scp dl380g10:/home/rob/.munge-key.b64 /home/rob/.munge-key.b64
scp /home/rob/.munge-key.b64 sparklina:/home/rob/.munge-key.b64

# 3. on each Spark
ssh -t sparky     sudo bash /home/rob/prismabuild/fleet/slurm/install.sh
ssh -t sparklina  sudo bash /home/rob/pb-slurm/install.sh
```

A Spark's run installs that key, stamps its sha256 beside it, and shreds the
copy. Do not carry the key through `/mnt/shared`: it is the fleet's shared
secret and an NFS export is the wrong place for one. Do not stage it in `/tmp`,
which an out-of-memory event cleared on this fleet once already.

The controller's own copy is not shredded by anything, because dl380g10 is
where it is created. Once both Sparks are installed, remove it yourself:

```bash
ssh dl380g10 shred -u /home/rob/.munge-key.b64
```

**Installing the `munge` package puts a key on the box by itself.** Measured
2026-09-05 in an `ubuntu:24.04` container: after `apt install munge`,
`/etc/munge/munge.key` exists, 128 bytes, `munge:munge`, generated by the
package's postinst. It is a good key and it is the wrong one, because it is
that box's alone. So "a key is present" is not evidence the fleet's key is
installed, and on a Spark `install.sh` treats a key without its stamp as a
refusal rather than as a step already done.

### What it refuses, and why each refusal is worth having

- **A `slurm` user at any uid but 64030.** The controller and the nodes
  exchange state as that user; a mismatch fails later, elsewhere, in a message
  that names neither box.
- **A SLURM that is not 25.11.x.** A 25.11 controller accepts slurmd from
  25.05, 24.11 and 24.05 only. `slurmd -V` is read after the packages install
  and needs no `slurm.conf` to answer.
- **A box whose `slurmd -C` disagrees with its own `NodeName=` stanza** on
  `CPUs`, `SocketsPerBoard`, `CoresPerSocket` or `ThreadsPerCore`. This is the
  original runbook's step 5 turned into a gate. Guessing a socket and core
  layout is how a node comes up `DRAINED` with `Low socket*core*thread count`
  and no obvious cause, and `task/affinity` binds against the declared
  layout. `RealMemory` is
  deliberately not compared: it is the fleet's admission budget from
  `fleet_boxes.json`, well under physical memory, and not the number
  `slurmd -C` reports.
- **A Spark with no packages at `/home/rob/slurm-build/arm64-24.04`.**

The topology check runs before anything is written under `/etc/slurm`, so a box
that does not match its stanza is refused rather than configured and then
refused. If it fires, fix `fleet/slurm/slurm.conf` in the repository, not the
copy on the box: all three boxes run an identical file, and a controller and a
node that disagree about `slurm.conf` produce errors that name neither.

### What it leaves behind

- the `slurm` user and group at uid/gid 64030, and
  `/var/spool/slurm/{ctld,d}` plus `/var/log/slurm` owned by them
- munge installed, the fleet's key at `/etc/munge/munge.key` mode 0400, and
  `/etc/munge/prismabuild-fleet-key.sha256` recording which key that is
- SLURM 25.11.2 from apt on dl380g10, from the prebuilt debs on the Sparks
- four configuration files in `/etc/slurm/`: `slurm.conf`, `gres.conf`,
  `cgroup.conf` and `epilog.sh`. Device containment needs no allow-list file:
  `ConstrainDevices=yes` in `cgroup.conf` does the work, and on cgroup v2 that
  is an eBPF program which denies exactly the GRES `File=` devices a job was
  not allocated and admits everything else
- `/mnt/shared/prismabuild-fleet/slurm/jobs`, mode 1777, created **as rob**:
  dl380g10 exports that dataset without `no_root_squash`, so root is `nobody`
  there and its `mkdir` would fail silently
- `munge` enabled and running, `slurmd` on every box, `slurmctld` on dl380g10

If a node stays down, read `/var/log/slurm/slurmd.log` on that node first.

## Phase 1 check: run `verify.sh`

Run it as rob from sparky, once all three installs are done. No sudo. It needs
a checkout (row 7 runs a real `pbrun`) and it reads `/etc/slurm/slurm.conf` on
the other two boxes over ssh, so sparky is the box that can do both:

```bash
cd /home/rob/prismabuild
fleet/slurm/verify.sh
```

It prints PASS or FAIL per row and stops at the first failure, because the rows
after a failure are being run against a fleet in a state nobody described. On
success it writes `~/.prismabuild/slurm-verify-passed.json`, which
`cutover.sh` looks for.

The rows, and what each one is really asking:

| Row | Claim |
|---|---|
| 0 | this box's `/etc/slurm/slurm.conf` is the file in the checkout |
| 0b | so does every other box, read over ssh; a controller and a node running different files produce errors that name neither |
| 1a | all three nodes registered and idle |
| 1b | each node offers exactly the `Gres` `slurm.conf` declares |
| 1c | each node advertises exactly the `Feature`s it declares, its own hostname among them |
| 2 | `sbatch --wait` runs a job on partition `cpu` and on partition `gpu` |
| 3 | a `--gres=shard:1` job sees the GPU |
| 4 | a job with no GRES runs on a Spark and cannot see or open the GPU |
| 5 | a job's processes are in a `job_<id>` cgroup |
| 6 | a shard job lands on a Spark; an untagged CPU job lands on dl380g10; `--constraint=x86` lands there too |
| 7 | `pbrun --transport slurm --here` runs one real action end to end |
| 8 | the lane's `jobs/` directory holds no orphaned state file |

Row 1c matters more than it looks: `pbrun --here` and a box-local checkout both
produce a hostname tag, and the lane turns tags into `--constraint`. Without the
hostname as a Feature, every pinned action is unschedulable.

Row 4 is the claim no container could test, and it is written to discriminate
rather than to assert. A job that failed to launch also sees no GPU, and that is
a broken lane, not device containment -- so the row checks that the job ran, and
that it ran on a Spark, before reading anything into the absence. It asks twice,
through `nvidia-smi -L` and by opening `/dev/nvidia0` directly. `nvidia-smi -L`
opens that device itself, so it does exercise the eBPF deny; the bare open is
the same question with nothing between it and the kernel, and a disagreement
between the two would be worth knowing about.

Row 5 is the check most likely to fail. `core._collect_worker_evidence` attests
`SLURM_JOB_ID` against this process's cgroup membership before it will run
anything, so a job whose processes are not in a `job_<id>` cgroup is refused by
the worker rather than by SLURM. `ProctrackType` is what to look at.

Row 7 uses `--here`, and that is load-bearing at this point in the install.
`pbrun` builds the job script around its own location: `RUNTIME_ROOT =
generation_root(__file__)`, so a job submitted from `/home/rob/prismabuild`
execs `/home/rob/prismabuild/tools/fleet/slurm_job.py` on whichever node the
scheduler picks, and that path is one box's local checkout. `--here` adds the
submitting box's hostname as a required tag, the tag matches that node's
`Feature`, and the job lands where the path exists. A cross-box submission such
as `--tag x86` from a Spark would land on dl380g10 and die with `No such file or
directory`, which reads like a broken lane and is not one.

The pull queue has the same property and lives with it, because agents run the
*published* `pbrun` under `/mnt/shared/prismabuild-fleet/runtime/<generation>/`,
a path every box mounts at the same place. So the cross-box check is meaningful
only after the cutover has published the runtime, and it belongs there:

```bash
# after the cutover, from the published path
/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbrun.py \
    --transport slurm --tag x86 --timeout-s 600 -- /bin/echo hello from slurm
```

Row 8 is the other half of the Epilog's job, the half that fails silently. A
`<job id>.job` file left behind for a job that has finished usually means the
Epilog could not delete it, which on this fleet means `root_squash` on the NFS
export. The row skips files whose jobs are still in `squeue`, so only an orphan
fails it, and it prints each orphan's `JobState` because one kind of orphan is
not a defect: **a node power-cycled mid-job runs no Epilog at all**, so its
state file survives. A file whose job is `COMPLETED` or `NODE_FAIL`, or which
the controller has forgotten because `MinJobAge` expired, is a reboot leak; it
is safe to delete by hand:

```bash
rm /mnt/shared/prismabuild-fleet/slurm/jobs/<job id>.job
```

Anything else -- a job that ended while the node stayed up -- is the export.

One check `verify.sh` does not do, because it leaves a container behind if it
goes wrong: cancelling a job that started a container, to watch the Epilog
remove it. Do that by hand once, when you have a minute:

```bash
tools/fleet/pbrun.py --transport slurm --here --timeout-s 600 \
    -- docker run -d --rm alpine sleep 300 &
sleep 20
tools/fleet/pbrun.py --transport slurm --withdraw <key prefix>
docker ps --filter label=prismabuild.action     # must list nothing
```

## Phase 2: cut over

Do this only after `verify.sh` passes, and in a campaign-quiet window. It is
Rob's call and Rob's to run.

```bash
cd /home/rob/prismabuild
fleet/slurm/cutover.sh --dry-run --yes     # read the plan
fleet/slurm/cutover.sh --yes               # do it
```

Run it from sparky. It reaches every box by ssh, and sparky is the only box
that can resolve the other two by name.

Nothing in it needs root. The loops are rob's processes, the crontab is rob's,
`pqwork.service` is rob's user unit, and the runtime generation is rob's to
publish.

It refuses unless all five of these hold:

1. `verify.sh` passed -- its marker, or `--verified` if you ran it on another
   box, because the marker is box-local.
2. `/mnt/shared/prismabuild-fleet/pb-queue/claimed` and `.../ready` are both
   empty. A stopped loop leaves its claim behind for a reaper that will not run
   again, and an item in `ready` is an action no SLURM job will ever pick up.
   Wait, or withdraw it with `pbrun --withdraw <key prefix>`.
3. `publish_runtime.py --dry-run --default-transport slurm` succeeds from this
   checkout. Step 5 below is the only step with no cheap retry -- by the time
   it runs, cron is edited and every loop on all three boxes is dead -- and
   `publish_runtime.py` refuses a dirty tree. Commit or stash first;
   `git status` must be clean where you run this.
4. No `pbrun` is waiting anywhere in the fleet. Each one is somebody watching
   for a result the loops are about to stop producing.
5. `--yes`.

Then, in this order, and the order is not arrangeable:

1. **The crontab, on all three boxes.** The supervisors are kept alive by a
   per-user crontab entry:

   ```
   */5 * * * * /usr/bin/python3 \
       /mnt/shared/prismabuild-fleet/repo/tools/supervise.py --ensure \
       >> /home/rob/tmp/pb-supervisor.log 2>&1
   ```

   Killing a supervisor without removing that line buys five minutes. The whole
   crontab is backed up verbatim to `~/.prismabuild/crontab.pre-cutover` on each
   box, so rollback restores what was there rather than reconstructing it.
2. **The supervisors.** Killing worker loops while a supervisor lives buys
   thirty seconds; it respawns them to the count `fleet_boxes.json` declares.
3. **The worker loops.** SIGTERM, a bounded wait, then SIGKILL.
4. **`pqwork.service` on both Sparks**, with `systemctl --user stop`. It is a
   *user* unit and takes no sudo. It is stopped, not disabled, so a reboot
   starts it again; stop it again after a reboot, or disable it deliberately.
5. **The runtime generation**, published with
   `publish_runtime.py --default-transport slurm`.

Processes are found the way `supervise._live_loops` finds them -- argv[0] is an
interpreter and argv[1] is the script -- and killed by pid. A `pkill -f
supervise.py` would match the ssh command carrying it, and a
`pkill -f tools/fleet/supervise.py` matches nothing at all: the live processes
run the published path, `.../repo/tools/supervise.py`, and on dl380g10 the
relative `repo/tools/supervise.py`.

### What the cutover retires and does not replace

The pull queue's worker loops carry a per-box `--timeout-s` budget from
`fleet_boxes.json` -- 7200 s on each Spark, 3600 s on dl380g10 -- and it bounds
every action, including actions whose submitter asked for no limit. Under SLURM
there is no equivalent. `--time` is sent only when the submitter passes
`--timeout-s`, and every partition is `MaxTime=UNLIMITED`, so an action
submitted with no limit runs until it finishes or somebody cancels it.

That is Rob's decision rather than a gap to close: elapsed time is not evidence
that a worker is dead, and a job making progress is never killed for taking
long. A stalled job is meant to be *reported*, by the liveness work, not
punished by a clock. Say it out loud here because the budgets disappear
silently otherwise: nothing in the SLURM lane inherits them.

### The default transport is a property of the published bytes

Cutover used to be described as `export PRISMABUILD_TRANSPORT=slurm`. That
describes one shell and no part of this fleet: agents start `pbrun` from a
crontab, from user units and from each other on three boxes, and there is no
single environment to export into.

So the default rides in the runtime generation instead.
`publish_runtime.py --default-transport slurm` records `default_transport` in
`RUNTIME_VERSION.json`, and `fleet_submit.default_transport()` reads the
environment first, then that field, then falls back to `pool`. The field is
optional and absent means the pull queue, so every generation published before
the cutover keeps the behaviour it had. `PRISMABUILD_TRANSPORT` still overrules
it for one process, which is what you want when you are testing.

## Rollback

```bash
cd /home/rob/prismabuild
fleet/slurm/rollback.sh --dry-run
fleet/slurm/rollback.sh
```

Run it from sparky, for the same reason as the cutover: it reaches every box by
ssh, and sparky is the only box that can resolve the other two by name.

It reads the state file `cutover.sh` wrote -- the newest
`~/.prismabuild/cutover-*.json` unless `--state` names another -- and reverses
it in reverse order, runtime first:

1. point the live runtime back at the generation the cutover replaced, with
   `publish_runtime.py --activate-generation <name>`. This is not a
   re-publication: that generation's bytes and receipt were proved when it was
   published, and publication never deletes a generation. Restoring it restores
   the previous default transport in the same atomic namespace operation that
   changed it.
2. restore each box's crontab from its verbatim backup
3. start `pqwork.service` again on both Sparks
4. start one supervisor per box now, rather than waiting up to five minutes for
   cron

The runtime goes first deliberately. Between step 1 and step 4 the fleet has no
workers and the default is the pull queue, so submissions queue and wait --
which is a fleet that is idle. The other order gives a window where workers
drain the queue while producers are still being told to use SLURM.

SLURM jobs already running keep running. Cancel the ones you do not want with
`pbrun --transport slurm --withdraw <key prefix>`, which reads the recorded job
id and calls `scancel`. You can leave `slurmctld` and `slurmd` running; with no
submissions they do nothing.

## Liveness: a running job is reported, never killed on elapsed time

`pbrun --transport slurm` sends no `--time` unless you pass `--timeout-s`.
A job that is doing something and is not visibly dead runs until it ends.
What the lane does instead of a wall-clock bound is measure, at a bounded
cadence, whether the job is doing anything, and tell you when it is not.

**What liveness is.** While a job is `RUNNING`, the submitting `pbrun` takes one
sample every 30 s (`LIVENESS_SAMPLE_S`, which is `JobAcctGatherFrequency`,
the rate at which `jobacct_gather/cgroup` refreshes the numbers; asking more
often reads the same gather twice). A sample is `progressing` when any of
these changed since the previous sample: CPU time, RSS, disk bytes read or
written, or the size of the job's `.out` or `.err`. `stalled_since` is the
time of the first sample in the current run of unchanged samples, or null.

**What it reads.**

- `sstat -j <jobid> -a -P -n --noconvert
  --format=JobID,AveCPU,MinCPU,MaxRSS,MaxDiskRead,MaxDiskWrite,NTasks,TRESUsageInTot`.
  (`TotalCPU` is an `sacct` field; `sstat` 25.11.2 refuses it. `AveCPU` and
  the `cpu=` entry of `TRESUsageInTot` are both `[DD-]HH:MM:SS`; the smoke's
  real line read `cpu=00:00:00`. `MaxRSS` is kept in the unit `--noconvert`
  prints, which the smoke showed to be bytes, and the sample calls it `rss`
  rather than asserting a unit.)
  This is the job's own cgroup accounting on the node, so an action does
  nothing to be measured and the numbers come from where the work runs.
  `sstat` reads running steps from `slurmd` and works without `slurmdbd`;
  smoke row 13 confirms it on this configuration.
- `stat` of `<lane root>/<action key>/<jobid>.out` and `.err`. This is the
  only evidence left when `sstat` is absent or refuses, and the sample says
  so in `sstat_error` and `evidence`.
- Nothing about the GPU. Per-process GPU telemetry on GB10 reads null, so
  the lane does not claim it. `slurm_lane.gpu_power_sample` is the seam for
  a board-power read against the envelope, the fleet's one honest GPU load
  signal, and returns null until it exists.

**Where it goes.** Every sample is one JSON line appended to
`<lane root>/<action key>/liveness.jsonl`. The file is append-only and is
never rewritten (issue #16 measured a 69 s NFS stall on a file rewritten
every heartbeat). When the job ends, the outcome record under
`pb-queue/done/` or `failed/` carries the latest sample and `stalled_since`
under `detail.liveness`; a withdrawal record carries the last sample too.
`slurm_lane.read_liveness(key, root)` returns the last line for any tool
that wants "stalled since" without parsing the file.

**When it speaks.** After 120 s of unchanged samples (`STALL_WINDOW_S`:
`ceil(69 s / 30 s) + 1` cadences, so that the samples across the window
cannot all sit inside one NFS stall, and so that an unchanged CPU count spans
at least two full accounting intervals), `pbrun` prints one line to stderr
and repeats it every ten minutes while the stall lasts:

```text
pbrun: <key12> slurm job <id> has shown no progress for <N> min on <node>; it is still running. Withdraw with pbrun --withdraw <key12> if it is dead.
```

**What it never does.** It never cancels a job. Not after the window, not
after any number of repeats. A stalled job ends in one of three ways: it
finishes; you decide it is dead and run
`pbrun --transport slurm --withdraw <key prefix>`; or it reaches a
`--timeout-s` you asked for at submission, which becomes `--time` and is
enforced by SLURM exactly as before. Elapsed time on its own is never
treated as evidence of death, and a job that is sleeping on a lock or waiting
on a network is reported, not judged.

## Reference: what the lane sends

For a GPU-slot action with two tags and a two-hour timeout, `pbrun --transport
slurm --gpu --tag gb10 --tag sparklina --timeout-s 7200 -- <command>` submits:

```text
sbatch --parsable --no-requeue --export=NIL \
    --job-name=pb-<first 12 of the action key> \
    --chdir=/mnt/shared/prismabuild-fleet/slurm/<action key> \
    --output=/mnt/shared/prismabuild-fleet/slurm/<action key>/%j.out \
    --error=/mnt/shared/prismabuild-fleet/slurm/<action key>/%j.err \
    --time=02:00:00 --mem=16384M --cpus-per-task=1 \
    --gres=shard:1 --constraint=gb10&sparklina \
    /mnt/shared/prismabuild-fleet/slurm/<action key>/job.sh
```

For a CPU action, the `--gres` flag is absent entirely, and `--constraint`
carries whatever tags the checkout's location produced.

`--exclusive` sends `--gres=gpu:1` rather than a larger shard count. Asking for
the whole device and asking for shards of it are mutually exclusive requests
against one GPU, so exclusivity is a different GRES name, not a bigger number.

Each submission seals a record next to the script at
`<lane root>/<action key>/submissions/<published unix>-<attempt>.json`, with
the job id and the exact argv, and points `<action key>/latest.json` at the
newest attempt. The generation is part of the name because the action key is a
content hash: asking for the same work twice is the same key and the same
directory, and a record named by the attempt alone collided across runs, which
refused every re-submission -- including the CAS hit that is the point of a
content-addressed build. `pbrun --withdraw`
reads `latest.json` to find the job to cancel.

## Reference: files this install touches

| Path | What it is |
|---|---|
| `/etc/slurm/slurm.conf` | Nodes, partitions, scheduling, `MinJobAge`, `Epilog` |
| `/etc/slurm/gres.conf` | The GPU and its shards, per node, no autodetection |
| `/etc/slurm/cgroup.conf` | Core, memory, and device containment |
| `/etc/slurm/epilog.sh` | Node-side cleanup of containers and checkouts |
| `/etc/munge/munge.key` | The fleet's shared authentication secret |
| `/etc/munge/prismabuild-fleet-key.sha256` | Which key that is, so a package-generated one is not mistaken for it |
| `/home/rob/pb-slurm/` | The copy of `fleet/slurm/` on dl380g10 and sparklina, which have no checkout |
| `/var/spool/slurm/` | Controller state and slurmd spool, local disks only |
| `/mnt/shared/prismabuild-fleet/slurm/` | Job scripts, submission records, job logs |
| `/mnt/shared/prismabuild-fleet/slurm/jobs/` | One state file per running job, for the Epilog |
| `/home/rob/.munge-key.b64` | The key in transit, created on dl380g10 and shredded on each Spark |
| `~/.prismabuild/slurm-verify-passed.json` | `verify.sh` passed here; `cutover.sh` looks for it |
| `~/.prismabuild/crontab.pre-cutover` | Each box's crontab as it was, for `rollback.sh` |
| `~/.prismabuild/cutover-<unix>.json` | What the cutover replaced, for `rollback.sh` |
