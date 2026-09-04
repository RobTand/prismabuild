# SLURM runbook, 2026-09-04

This runbook installs SLURM across the three fleet boxes and cuts `pbrun` over to
it. Every command that needs root is marked `sudo`. Run the commands in the
order given; each section ends with a check that has to pass before you move on.

The code this configures is already merged and tested: `prismabuild.slurm_lane`
(submit, wait, cancel), `tools/fleet/slurm_job.py` (what a batch job runs), and
`pbrun --transport slurm`. The pull queue is untouched and remains the default.

## Not yet verified

SLURM is installed on no box in this fleet. Everything below was written against
the SchedMD documentation, the fleet's measured budgets in
`tools/fleet/fleet_boxes.json`, and tests that drive fake `sbatch`, `sacct`,
`scontrol`, `squeue`, and `scancel` executables. Nothing here has run against a
controller.

These items in particular need checking during the install, because each one has
a plausible way to be wrong that only a real controller can reveal:

1. **The `shard` syntax in `gres.conf`.** The lane asks for `--gres=shard:N`, and
   `gres.conf` binds shards to a device with
   `Name=shard Count=N File=/dev/nvidia0`. If `slurmd -C` or the slurmd log
   rejects that form, the alternative is the same line without `File=`. Check it
   before starting the daemons, not after.
2. **The cgroup attestation that the worker performs.**
   `core._collect_worker_evidence` refuses a partial SLURM environment and then
   attests `SLURM_JOB_ID` against `/proc/self/cgroup`, requiring exactly one
   `job_<id>` path component. With `ProctrackType=proctrack/cgroup` that should
   hold. If it does not, every action fails inside the worker with `SLURM
   environment is not attested by this process's cgroup membership`, and the
   failure looks like the action's fault rather than the scheduler's. The
   hello-world check below is written to catch this.
3. **`--export=NIL` and Git.** A batch job gets SLURM's own variables and nothing
   else, so `HOME` and `PATH` are absent while `git` materializes the checkout.
   Git tolerates a missing `HOME`. If it does not on Ubuntu 26.04, the fix is to
   add `HOME` to the job through `slurm_lane.job_script_text`.
4. **The device allowlist.** `cgroup_allowed_devices_file.conf` lists the NVIDIA
   control devices that CUDA needs to initialize. If a GPU job fails at CUDA
   init while a non-GPU job runs, that list is the first place to look.
5. **`CPUs=` for the two GB10 boxes.** `slurm.conf` says `CPUs=20`. Replace it
   with what `slurmd -C` reports on each box, as the install step below directs.
6. **Whether 25.11 built from source interoperates with 25.11.2 from apt.** Build
   the same patch version on the Sparks. A version skew inside 25.11 is not
   expected to matter, but it has not been observed here.
7. **Whether the Epilog can delete its own state file over NFS.** The Epilog runs
   as `root` on the compute node and removes
   `/mnt/shared/prismabuild-fleet/slurm/jobs/<job id>.job`, which the job wrote
   as `rob`. If dl380g10 exports that dataset with `root_squash`, the node's
   `root` maps to `nobody` and the delete fails. The Epilog swallows the error
   and still exits 0, as it must, so the symptom is silent: state files
   accumulate in `jobs/`. Check that directory after the validation runs below.
   The fix is a `no_root_squash` export for that path, or writing the state file
   world-writable.

## What this replaces, and what it does not

`pool.py` and `tools/fleet/worker_loop.py` implement a pull queue over NFS:
workers claim sealed actions by `rename()`, hold a lease, and admit work against
a memory and GPU-slot budget. SLURM replaces the claiming, the leasing, and the
admission. It does not replace anything in `core.py`: action keys, the CAS,
receipts, attestation, and the git-bundle checkout snapshot are the same objects
under either transport, and `slurm_job.py` materializes a snapshot through the
same `prismabuild.materialize` code that a pull-queue worker runs.

Two capabilities of the pull queue have no equivalent yet:

- **Aging.** `pool.py` counts denials in `passes` and lets a starved item
  withhold a host past `STARVATION_FLOOR`. Age-based priority in SLURM needs
  `priority/multifactor`, which needs `slurmdbd`, which this install defers. Until
  then the fleet runs FIFO within a priority, plus backfill.
- **`sacct`.** With `AccountingStorageType=accounting_storage/none`, `sacct`
  fails for every job. The lane tries it first anyway and falls back to
  `scontrol` and `squeue`, so deploying `slurmdbd` later is a configuration
  change and not a code change.

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
