# SLURM runbook, 2026-09-04

This runbook installs SLURM across the three fleet boxes and cuts `pbrun` over to
it. Every command that needs root is marked `sudo`. Run the commands in the
order given; each section ends with a check that has to pass before you move on.

The code this configures is already merged and tested: `prismabuild.slurm_lane`
(submit, wait, cancel), `tools/fleet/slurm_job.py` (what a batch job runs), and
`pbrun --transport slurm`. The pull queue is untouched and remains the default.

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
  Nothing to do at install except read `jobs/` once after the validation runs.
- **`CPUs=` for the two GB10 boxes** (was item 5). Measured and written into
  `slurm.conf`: 20 CPUs as one socket of twenty, one thread per core. Step 5's
  `slurmd -C` is now a cross-check, not the source.
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
is SLURM's own, built from its `SLURM_*` variables, so
`PRISMABUILD_EPILOG_DOCKER` and `PRISMABUILD_SLURM_LANE_ROOT` are test-only
levers and not something to set at install time.  The Epilog finds `docker` on
`PATH`.

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
   opens. Whether `cgroup_allowed_devices_file.conf` admits exactly the right
   NVIDIA control interfaces -- `/dev/nvidiactl`, the UVM pair,
   `nvidia-modeset` and `nvidia-caps/nvidia-cap1,2`, all measured present on
   both GB10 boxes -- is answerable only on a box with a GPU. If a GPU job
   fails at CUDA init while a non-GPU job runs, that list is the first place to
   look. Note also that the fleet's 25.11.2 build ships no `gpu_nvml.so`, so
   `AutoDetect=nvml` is not available at all and `gres.conf` must stay static.
3. **NFS `root_squash` end to end.** The export is measured and the Epilog is
   fixed, but the fix has been exercised only against a fake `runuser` and a
   local bind mount. The first killed job on the real fleet is the test.
4. **Three-box RPC.** One node cannot show a controller talking to a remote
   `slurmd`, a node draining and returning under `ReturnToService=2`, or an
   action landing on a box other than the submitter's.
5. **Whether 25.11 built from source interoperates with 25.11.2 from apt** (was
   item 6). The Sparks' packages are a rebuild of Ubuntu 26.04's own source
   package at the same patch version, and both ends have now been exercised
   separately; they have not been exercised against each other.

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
  `priority/multifactor`, which needs `slurmdbd`, which this install defers. Until
  then the fleet runs FIFO within a priority, plus backfill.
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
| sparky | GPU node | aarch64, Ubuntu 24.04 | Built from source, 25.11.x |
| gx10-6b77 (sparklina) | GPU node | aarch64, Ubuntu 24.04 | Built from source, 25.11.x |

The Sparks need a source build because Ubuntu 24.04 ships slurm-wlm 23.11.4, and
a 25.11 controller accepts nodes running 25.05, 24.11, or 24.05 only.

## Step 1: create the `slurm` user on all three boxes

The user and group must have the same numeric IDs everywhere, because the
controller and the nodes exchange state as that user.

Run on each of dl380g10, sparky, and gx10-6b77:

```bash
sudo groupadd -g 64030 slurm
sudo useradd -u 64030 -g slurm -s /usr/sbin/nologin -M -d /var/spool/slurm slurm
sudo mkdir -p /var/spool/slurm/ctld /var/spool/slurm/d /var/log/slurm
sudo chown -R slurm:slurm /var/spool/slurm /var/log/slurm
sudo chmod 755 /var/spool/slurm
```

Check that `id slurm` reports `uid=64030` on all three boxes. If a box already
has a `slurm` user at a different ID, stop and reconcile it before continuing.

## Step 2: install munge and share one key

Every box authenticates to every other box with the same munge key.

Install munge on all three boxes:

```bash
sudo apt update
sudo apt install -y munge libmunge-dev
```

On dl380g10, create the key and read it back:

```bash
sudo /usr/sbin/mungekey --create --force
sudo chown munge:munge /etc/munge/munge.key
sudo chmod 400 /etc/munge/munge.key
sudo base64 /etc/munge/munge.key
```

Copy that base64 text to each Spark and install it there:

```bash
mkdir -p /home/rob/tmp
cat > /home/rob/tmp/munge.key.b64 <<'EOF'
<paste the base64 text here>
EOF
sudo base64 -d /home/rob/tmp/munge.key.b64 | sudo tee /etc/munge/munge.key >/dev/null
rm -f /home/rob/tmp/munge.key.b64
sudo chown munge:munge /etc/munge/munge.key
sudo chmod 400 /etc/munge/munge.key
```

Two paths to avoid. Do not copy the key through `/mnt/shared`: the key is the
fleet's shared secret and an NFS export is the wrong place for it. Do not stage
it in `/tmp`, which an out-of-memory event cleared on this fleet once already.

Start munge on all three boxes and check it:

```bash
sudo systemctl enable --now munge
munge -n | unmunge | head -n 5
```

Then check cross-box authentication from dl380g10:

```bash
munge -n | ssh sparky unmunge | grep STATUS
munge -n | ssh gx10-6b77 unmunge | grep STATUS
```

Both must report `STATUS: Success (0)`. A failure here is a wrong key or a clock
skew larger than munge's window; fix it before installing SLURM.

## Step 3: install SLURM on dl380g10

```bash
sudo apt install -y slurm-wlm slurmctld slurmd
apt-cache policy slurm-wlm | sed -n 2p
```

Record the exact installed version. The Sparks must build that same version.

## Step 4: build SLURM 25.11 on each Spark

Run this on sparky, then on gx10-6b77. Substitute the version that dl380g10
reported in step 3 for `25.11.2` throughout.

Install the build tooling:

```bash
sudo apt install -y build-essential fakeroot devscripts equivs wget
```

Fetch and unpack the release:

```bash
mkdir -p /home/rob/tmp/slurm-build && cd /home/rob/tmp/slurm-build
wget https://download.schedmd.com/slurm/slurm-25.11.2.tar.bz2
tar -xaf slurm-25.11.2.tar.bz2
cd slurm-25.11.2
```

SchedMD has shipped an in-tree `debian/` directory since 23.11.0, so the release
tarball builds Debian packages directly. Install its declared build dependencies,
then build:

```bash
sudo mk-build-deps -i -t "apt-get -y" debian/control
debuild -b -uc -us
```

The build takes a while on a GB10 and writes `.deb` files to the parent
directory. Install them:

```bash
cd /home/rob/tmp/slurm-build
sudo apt install -y ./slurm-smd_*.deb ./slurm-smd-client_*.deb \
    ./slurm-smd-slurmd_*.deb ./slurm-smd-libslurm-perl_*.deb
```

Check the version, and check it against the controller:

```bash
slurmd -V
```

Do not write `/mnt/shared` during any of this. The build stays under
`/home/rob/tmp`.

## Step 5: read each node's real topology

`slurm.conf` in this repository carries `CPUs=20` for the two GB10 boxes as a
placeholder. Guessing a socket and core layout is how a node comes up `DRAINED`
with `Low socket*core*thread count` and no obvious cause.

On each of the three boxes:

```bash
sudo slurmd -C
```

Copy the `NodeName=` line it prints. In the next step, replace the `CPUs=` value
in that box's stanza with what `slurmd -C` reported, and add its
`Sockets=`/`CoresPerSocket=`/`ThreadsPerCore=` values if they differ from a flat
20 cores. Leave `RealMemory` alone: it is the fleet's admission budget from
`fleet_boxes.json`, deliberately well under physical memory, and not the number
`slurmd -C` reports.

## Step 6: install the configuration on all three boxes

The configuration lives in this repository at `fleet/slurm/`. Copy it to every
box; all three run identical `slurm.conf`, `gres.conf`, and `cgroup.conf`.

From a checkout on each box:

```bash
sudo install -m 644 fleet/slurm/slurm.conf  /etc/slurm/slurm.conf
sudo install -m 644 fleet/slurm/gres.conf   /etc/slurm/gres.conf
sudo install -m 644 fleet/slurm/cgroup.conf /etc/slurm/cgroup.conf
sudo install -m 644 fleet/slurm/cgroup_allowed_devices_file.conf \
    /etc/slurm/cgroup_allowed_devices_file.conf
sudo install -m 755 fleet/slurm/epilog.sh   /etc/slurm/epilog.sh
```

Apply the `CPUs=` correction from step 5 to `/etc/slurm/slurm.conf` on every box,
so that all three copies stay identical. A controller and a node that disagree
about `slurm.conf` produce errors that name neither file.

Create the lane's directories on the shared mount, once, from any box:

```bash
mkdir -p /mnt/shared/prismabuild-fleet/slurm/jobs
chmod 1777 /mnt/shared/prismabuild-fleet/slurm/jobs
```

The `jobs` directory holds one small state file per running job. The Epilog runs
as root on the node and reads that file to find the containers and the checkout a
killed job left behind.

## Step 7: start the daemons

On dl380g10:

```bash
sudo systemctl enable --now slurmctld
sudo systemctl enable --now slurmd
sudo systemctl status slurmctld --no-pager | head -n 5
```

On each Spark:

```bash
sudo systemctl enable --now slurmd
sudo systemctl status slurmd --no-pager | head -n 5
```

If a node stays down, read `/var/log/slurm/slurmd.log` on that node first. The
two failures to expect are a `gres.conf` line the version rejects, and a
topology mismatch from step 5.

## Step 8: validate

Run these from dl380g10 unless a step says otherwise.

Check that all three nodes registered and are idle:

```bash
sinfo -N -l
scontrol show node sparky
scontrol show node gx10-6b77
scontrol show node dl380g10
```

Each Spark must report `Gres=gpu:1,shard:2` (sparky) or `Gres=gpu:1,shard:3`
(gx10-6b77), and `AvailableFeatures` must include its hostname. The hostname
matters: `pbrun --here` and a box-local checkout both produce a hostname tag, and
the lane turns tags into `--constraint`. Without the hostname as a feature, every
pinned action is unschedulable.

Run a hello-world job on each partition:

```bash
sbatch --wait --partition=cpu --wrap='hostname; echo $SLURM_JOB_PARTITION'
sbatch --wait --partition=gpu --constraint=gb10 --wrap='hostname'
echo "exit status: $?"
```

`sbatch --wait` exits with the job's exit code, so a zero here means the job ran
and succeeded.

Check that a shard allocation reaches the GPU:

```bash
srun --partition=gpu --gres=shard:1 nvidia-smi -L
```

Check that a job with no GRES cannot reach the GPU. This is the rule
`ConstrainDevices=yes` enforces, and the one `pbrun` enforces from the other side
by masking `CUDA_VISIBLE_DEVICES`:

```bash
srun --partition=gpu nvidia-smi -L
```

That command should fail or list nothing.

Check the cgroup attestation that the PrismaBuild worker performs. This is item 2
of the unverified list, and it is the check most likely to fail:

```bash
srun --partition=gpu --gres=shard:1 bash -c 'grep -c "job_$SLURM_JOB_ID" /proc/self/cgroup'
```

The result must be `1`. Anything else means `core._collect_worker_evidence`
refuses every action, and `ProctrackType` is what to look at.

Finally, run one real action through the lane. Run it with `--here`, and from
the box you are standing on:

```bash
cd /home/rob/prismabuild
tools/fleet/pbrun.py --transport slurm --here --timeout-s 600 \
    -- /bin/echo hello from slurm
```

`--here` is load-bearing at this point in the install, and the reason is worth
stating. `pbrun` builds the job script around its own location: `RUNTIME_ROOT =
generation_root(__file__)`, so a job submitted from `/home/rob/prismabuild`
execs `/home/rob/prismabuild/tools/fleet/slurm_job.py` on whichever node the
scheduler picks. That path is one box's local checkout. `--here` adds the
submitting box's hostname as a required tag, the tag matches that node's
`Feature`, and the job lands where the path exists. A cross-box submission such
as `--tag x86` from a Spark would land on dl380g10 and die with `No such file or
directory` — which reads like a broken lane and is not one.

The pull queue has the same property and lives with it, because agents run the
*published* `pbrun` under `/mnt/shared/prismabuild-fleet/runtime/<generation>/`,
a path every box mounts at the same place. So the cross-box check is meaningful
only after step 9.4 publishes the runtime, and it belongs there:

```bash
# after the runtime is published, from the published path
/mnt/shared/prismabuild-fleet/runtime/repo/tools/fleet/pbrun.py \
    --transport slurm --tag x86 --timeout-s 600 -- /bin/echo hello from slurm
```

Either way, expect `pbrun: submitted <key> as slurm job <id>`, then the job's
output, then `pbrun: executed via slurm job <id> (COMPLETED)` and exit status 0.
If the job runs and exits 0 but `pbrun` reports `published no receipt`, the work
did not reach the CAS: read the `.out` and `.err` files in
`/mnt/shared/prismabuild-fleet/slurm/<action key>/`.

Check the Epilog by cancelling a job that started a container:

```bash
tools/fleet/pbrun.py --transport slurm --here --timeout-s 600 \
    -- docker run -d --rm alpine sleep 300 &
sleep 20
tools/fleet/pbrun.py --transport slurm --withdraw <key prefix>
docker ps --filter label=prismabuild.action
```

The last command must list nothing. Then check the other half of the Epilog's
job, the one that fails silently:

```bash
ls -l /mnt/shared/prismabuild-fleet/slurm/jobs/
```

That directory must be empty. A `<job id>.job` file left behind for a job that
has finished means the Epilog could not delete it, which on this fleet means
`root_squash` on the NFS export — item 7 of the unverified list.

## Step 9: cut over

Do this only after step 8 passes on all three boxes.

1. Check that the pull queue is idle. No action may be in flight when the loops
   stop, because a stopped loop leaves its claim behind for the reaper:

   ```bash
   ls /mnt/shared/pb-queue/ready /mnt/shared/pb-queue/claimed
   ```

   Both must be empty. If `claimed` is not empty, wait, or withdraw the action
   with `pbrun --withdraw`.

2. Stop the supervisors and worker loops on both Sparks and on dl380g10:

   ```bash
   sudo systemctl stop pqwork.service
   pkill -f tools/fleet/supervise.py
   pkill -f tools/fleet/worker_loop.py
   ```

   Run `tools/fleet/runtime_process_census.py` afterwards to confirm that no loop
   survived.

3. Switch the default transport. `pbrun` reads `PRISMABUILD_TRANSPORT`, so this
   belongs wherever the fleet's shell environment is set, for every agent and
   every user:

   ```bash
   export PRISMABUILD_TRANSPORT=slurm
   ```

4. Publish the runtime so that every box serves the same bytes. **This step is
   Rob's to run.** `tools/fleet/publish_runtime.py` rolls the atomic runtime
   generation, and publishing is a decision, not a deployment detail.

## Rollback

Rollback is two commands and no code change. The pull queue's code is untouched
by this work; only `pbrun` gained a branch, and its default is still `pool`.

1. Unset the transport:

   ```bash
   unset PRISMABUILD_TRANSPORT
   ```

2. Restart the worker loops from `tools/fleet/fleet_boxes.json`, the same way
   they were started before.

Any SLURM job still running keeps running. Cancel the ones you do not want with
`pbrun --transport slurm --withdraw <key prefix>`, which reads the recorded job
id and calls `scancel`. You can leave `slurmctld` and `slurmd` running during a
rollback; with no submissions, they do nothing.

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
`<action key>/submissions/<attempt>.json`, with the job id and the exact argv,
and points `<action key>/latest.json` at the newest attempt. `pbrun --withdraw`
reads `latest.json` to find the job to cancel.

## Reference: files this install touches

| Path | What it is |
|---|---|
| `/etc/slurm/slurm.conf` | Nodes, partitions, scheduling, `MinJobAge`, `Epilog` |
| `/etc/slurm/gres.conf` | The GPU and its shards, per node, no autodetection |
| `/etc/slurm/cgroup.conf` | Core, memory, and device containment |
| `/etc/slurm/cgroup_allowed_devices_file.conf` | NVIDIA control devices CUDA needs |
| `/etc/slurm/epilog.sh` | Node-side cleanup of containers and checkouts |
| `/etc/munge/munge.key` | The fleet's shared authentication secret |
| `/var/spool/slurm/` | Controller state and slurmd spool, local disks only |
| `/mnt/shared/prismabuild-fleet/slurm/` | Job scripts, submission records, job logs |
| `/mnt/shared/prismabuild-fleet/slurm/jobs/` | One state file per running job, for the Epilog |
