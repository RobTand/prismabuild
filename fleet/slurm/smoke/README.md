# The SLURM lane, run rather than described

Everything in `fleet/slurm/` and `src/prismabuild/slurm_lane.py` was written
against SchedMD's documentation and tested against fake `sbatch`, `sacct`,
`scontrol`, `squeue` and `scancel` executables. This harness is where a real
controller answers instead.

```
fleet/slurm/smoke/run.sh                                      # Ubuntu 24.04's 23.11.4
DEB_DIR=/home/rob/slurm-build/arm64-24.04 fleet/slurm/smoke/run.sh   # the fleet's 25.11.2
PRISMABUILD_SLURM_SMOKE=1 PYTHONPATH=src pytest -q tests/test_slurm_smoke.py
```

One privileged container, one node, one partition set. The repository is
mounted read-only; the CAS, `pb-queue`, the lane root and the actions' own
checkouts live in a per-run directory under `/home/rob/slurm-build/smoke/`,
mounted at `/mnt/shared` so `pbrun`'s fleet paths resolve without being
patched. The host's real `/mnt/shared` is never touched.

## What it establishes

| Row | Claim |
|-----|-------|
| 1a | `sinfo` shows the node idle offering `shard:2` |
| 1b | a plain `sbatch --wait --wrap=hostname` returns 0 |
| 2 | `pbrun --transport slurm` runs a git-snapshot action end to end, publishes a CAS receipt, and files `pb-queue/done/<key>.json` with `status=executed`, `detail.receipt_published`, the job id and the node |
| 3 | the same action submitted again re-executes nothing |
| 4 | a failing command files `failed/<key>.json` with a non-zero `detail.returncode` and a stderr tail |
| 5 | `--timeout-s` becomes `--time`, and SLURM -- not `pbrun` -- kills the job, which arrives as `detail.slurm.state=TIMEOUT` with `signal=15`, and which `pbrun` reports as `failed (TIMEOUT)` |
| 6 | `--withdraw` on a running job `scancel`s it, files the `withdrawn/` marker and a `failed/` record carrying `withdrawn_by` |
| 7a | `--gres=shard:1` schedules two jobs on a two-shard node and holds the third |
| 7b | `--constraint` for a Feature no node has is refused at submit and reported by `pbrun` |
| 8 | the Epilog ran for a killed job, matched containers by the action's ownership label, and removed its state file as the job's user rather than as root |
| 9 | with no `slurmdbd`, `sacct` answers nothing and the lane's provenance comes from `scontrol` |

## What it does not establish

The container is not the fleet, and four things stay open for the install:

- **Device containment.** `ConstrainDevices` is off here and the node's GRES is
  bound to a character device nothing opens. Whether
  `cgroup_allowed_devices_file.conf` admits exactly the right NVIDIA control
  interfaces is answerable only on a box with a GPU.
- **The fleet's cgroup arrangement.** Delegation here needs `--privileged`,
  `--cgroupns=private`, a manual `cgroup.subtree_control` and
  `IgnoreSystemd=yes`; the fleet's boxes have systemd and slurmd under it.
  What *is* established is that `proctrack/cgroup` puts a job's processes in a
  `job_<id>` cgroup that `core._verify_slurm_process_membership` accepts.
- **NFS.** The lane root here is a local bind mount, so `root_squash` -- the
  reason the Epilog removes its state file as `SLURM_JOB_USER` -- cannot be
  reproduced. `tests/test_slurm_epilog.py` covers that at the shell.
- **Three boxes.** One node cannot show controller/slurmd RPC across a version
  skew, a node draining and returning, or an action landing on a box other
  than the submitter's.

## The two SLURMs behave differently, and the differences are recorded

Both pass all eleven rows. Two things had to be worked around for 23.11.4, and
neither is a lane defect:

- Its `cgroup/v2` plugin creates its stepd scope under `/sys/fs/cgroup/system.slice`
  and refuses to initialize when that directory is absent (`Could not create
  scope directory .../pbsmoke_slurmstepd.scope`). On a box systemd owns it;
  `inside.sh` creates it. 25.11.2 does not need it.
- With `AccountingStorageType=accounting_storage/none`, every job is scheduled
  as `InvalidAccount` (`_refresh_assoc_mgr_qos_list: no new list given back`)
  and starts about thirty seconds later, once the association refresh fills in
  `Assoc=0` and backfill picks it up. 25.11.2 starts jobs immediately. Nothing
  fails; everything is slower, and a row that read a fixed interval would read
  a stalled fleet. That is an argument for putting the 25.11 packages on the
  nodes beyond the RPC-version one.

## Deviations from `fleet/slurm/slurm.conf`, and why

`inside.sh` generates the config rather than copying it, and prints every
deviation at the top of the run:

| Setting | Fleet | Here | Why |
|---------|-------|------|-----|
| `SlurmUser` | `slurm` | `root` | no `slurm` user in the image, and creating one would be testing `useradd` |
| `NodeName` | three boxes | the container | one node |
| `RealMemory` | the `fleet_boxes.json` budget | 16384 | enough for two concurrent shard jobs at the 4 GB default demand |
| `KillWait` | 30 | 10 | rows 5 and 6 would otherwise spend it waiting |
| `ConstrainDevices` | `yes` | `no` | there are no devices to constrain |
| `IgnoreSystemd` | absent | `yes` | there is no systemd to ask for a cgroup scope |
| `gres.conf` `File=` | `/dev/nvidia0` | `/dev/nvidia0`, a `mknod`'d character device | slurmd refuses `shard` with no `File=` on the sharing GRES; see below |

Every scheduler *choice* is the fleet's unchanged: `select/cons_tres` with
`CR_Core_Memory`, `proctrack/cgroup`, `task/cgroup,task/affinity`,
`jobacct_gather/cgroup`, `sched/backfill`, `priority/basic`, `MinJobAge=3600`,
`AccountingStorageType=accounting_storage/none`, and the real `epilog.sh`.

---

# Three nodes: `fleet/slurm/smoke/multinode/`

```
fleet/slurm/smoke/multinode/run.sh                        # the fleet's 25.11.2
PB_SMOKE3_SLURM=24.04 fleet/slurm/smoke/multinode/run.sh  # Ubuntu 24.04's 23.11.4
PRISMABUILD_SLURM_SMOKE=1 PYTHONPATH=src pytest -q tests/test_slurm_smoke_multinode.py
```

A docker network and three privileged containers whose hostnames are the
fleet's own NodeNames. `dl380g10` runs munged, slurmctld and slurmd as the
`cpu` node; `sparky` and `gx10-6b77` run munged and slurmd as `gpu` nodes with
a `mknod`'d `/dev/nvidia0` and the fleet's shard counts. One run volume is
mounted at `/mnt/shared` in all three, so the CAS, `pb-queue` and the lane root
are one filesystem as they are on the fleet; the repository is mounted
read-only in all three; the host's real `/mnt/shared` is never touched.

A passing run takes about four and a half minutes and leaves its transcript in
`/home/rob/slurm-build/smoke/multinode-<stamp>/smoke.log`.

Three things differ from the one-node harness beyond the node count:

- **The configuration is generated from the fleet's own files.** `genconf.py`
  reads `fleet/slurm/slurm.conf`, `gres.conf` and `cgroup.conf` and edits
  them, rather than writing a config from scratch. What the rows check -- the
  partition lines, the Features a `--tag` becomes, the node weights, the shard
  counts -- is exactly what those files say, and a smoke that restates them
  tests the restatement. One copy is generated and installed in all three
  containers, because slurmd sends a config hash at registration and identical
  bytes is the only way to be sure the controller's "different slurm.conf"
  complaint is absent because nothing differs.
- **The rows run on the host.** Three of them reach into a container that is
  not the submitter's, and the image's `docker` is the Epilog's fake one. So
  `rows_multinode.py` drives the containers with `docker exec` and reads the
  records back off the volume at its host path.
- **The action is `./action.sh`, not `bash action.sh`.** `pbrun` pins an action
  to the submitting box whenever argv[0] resolves to an executable that is
  neither in the checkout nor on shared storage, and `/usr/bin/bash` is one.
  Every one-node row therefore carried `--constraint=pbsmoke` without anybody
  noticing, because there the submitter was also the only node. Here it would
  make the scheduler's choice the submitter's.

## What it establishes

| Row | Claim |
|-----|-------|
| M1 | `sinfo -N` shows all three nodes idle with exactly the Gres and Features `fleet/slurm/slurm.conf` declares |
| M2 | an untagged CPU-only `pbrun` lands on `dl380g10` with `--partition=cpu`, no constraint, and no `--time` |
| M3 | a `--gpu` `pbrun` lands on a Spark with `--partition=gpu --gres=shard:1` |
| M4 | a `--tag gx10-6b77` CPU-only `pbrun` lands there with no `--partition`: the constraint decided |
| M9 | a `--tag cpu` `pbrun` lands on the CPU box, `cpu` being a node Feature and not only a partition |
| M10 | an `--anywhere` `pbrun` on an idle fleet still lands on the CPU box, on `Weight=1` against the Sparks' 10 |
| M11 | the same `--anywhere` action overflows to a Spark when `dl380g10` is held by an `--exclusive` job: a preference, not a pin |
| M5 | a `pbrun` submitted in the `sparky` container executes on `dl380g10`, and its output comes back through the shared volume |
| M6 | a node whose slurmd and job processes are killed under a running job ends it as `NODE_FAIL`, `pbrun` reports that state with no receipt, and the node returns to idle on `ReturnToService=2` with no operator action |
| M7 | the controller is restarted for sixty seconds under a running job, `pbrun` says it is waiting rather than reporting an ending, and the job completes and reaches the submitter |
| M8 | repeating M2 and M3 executes neither again |

Two facts the rows record rather than assert, because they are how the lane
works and not defects:

- **A repeat still costs a job id.** There is no pre-submit CAS short circuit;
  the lookup happens in the worker on the node that won the allocation, so the
  second submission is a real job that finds a receipt and publishes nothing.
- **`TERM` and `KILL` are different controllers.** M7 restarts slurmctld the
  way `systemctl restart` does, with `TERM`, and the job survives because the
  state save on shutdown carries it. A slurmctld killed with `-9` loses a job
  that started since its last periodic save: measured here, the recovered
  controller had JobId=6 as PENDING, logged `error: Registered PENDING JobId=6
  StepId=6.batch on node dl380g10`, and killed it as `non-startable ... Job
  credential revoked`. That is a fact about `StateSaveLocation` and a crash,
  not about a restart.

M7 is also where a lane defect was found. Before the fix, `wait` returned
`UNKNOWN` on the first poll it could not reach the controller for, so a
hundred-second restart made `pbrun` print `failed (UNKNOWN)` and exit 1 for a
job that went on to publish a receipt. A twenty-second outage proves nothing,
because SLURM's own client library absorbs it -- a `scontrol` issued while the
controller is down blocks about forty seconds and then answers. Sixty seconds
is what it takes for a poll to really come back with nothing.

## What it does not establish

- **Real GPUs.** `ConstrainDevices` is off and the Sparks' GRES binds a
  `mknod`'d character device nothing opens. Whether a `--gres=shard:1` job can
  open `/dev/nvidia0` and a job with no GRES cannot is answerable only on a box
  with a driver.
- **NFS `root_squash`.** The lane root here is a bind mount shared between
  containers, which makes it one filesystem but not a squashing one. The
  Epilog's delete as `SLURM_JOB_USER` is exercised; the reason it exists is
  not.
- **systemd cgroup delegation.** All three containers need `--privileged`,
  `--cgroupns=private`, a hand-written `cgroup.subtree_control` and
  `IgnoreSystemd=yes`. The fleet's boxes have systemd and slurmd under it.
  What three containers do establish is that this is three independent
  instances of one arrangement, not something that only worked because there
  was one of it.
- **A version skew between boxes.** All three run the same packages. Running
  25.11.2 on two nodes and 23.11.4 on the third would need two images and is
  not what this builds.
- **Wall-clock realism.** Three containers share one GB10's twenty cores and
  one clock. Nothing here says how the fleet behaves under load, and the
  `SlurmdTimeout=30` that makes M6 finish in under a minute is not the fleet's
  five.

## Deviations from `fleet/slurm/*.conf`, and why

`genconf.py` prints these at the top of every run. Everything not listed is the
fleet's file unchanged, including `SlurmctldHost=dl380g10`, all three
`NodeName` lines' `RealMemory`, `Gres`, `Feature` and `Weight`, all three
`PartitionName` lines, `ReturnToService=2`, `MinJobAge=3600`, every scheduler
and cgroup plugin choice, and the real `epilog.sh`.

| Setting | Fleet | Here | Why |
|---------|-------|------|-----|
| `SlurmUser` | `slurm` | `root` | no `slurm` user in the image, and creating one would be testing `useradd` |
| `NodeName=dl380g10` topology | `CPUs=80`, 2x20x2 | `CPUs=20`, 2x10x1 | all three containers are on one GB10; a configured topology larger than what slurmd reports comes up DRAINED with "Low socket*core*thread count" |
| `KillWait` | 30 | 10 | M6 would otherwise spend it waiting |
| `SlurmdTimeout` | absent, so 300 | 30 | M6 waits for the controller to notice a dead node; the value under test is `ReturnToService`, not this |
| `ConstrainDevices` | `yes` | `no` | the GRES binds a `mknod`'d character device nothing opens |
| `IgnoreSystemd` | absent | `yes` | there is no systemd to ask for a cgroup scope |
| `gres.conf` `File=` | the GB10's device | the same path, `mknod`'d | the file itself is unchanged; slurmd refuses a SHARED gres whose SHARING gres has no `File=` |

`RealMemory` is **not** a deviation, unlike in the one-node harness: 73728,
81920 and 61440 MiB are all under what slurmd reports inside a container on
this box, so the fleet's own budgets stand.

The munge key is baked into a derived image, generated per run, and the image
is removed when the run ends. That is what makes it one key in all three
containers with no ordering between them. It is test-only; on the fleet the
key is installed per box at mode 0400 and never travels through a build
context.
