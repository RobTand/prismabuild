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
| 4 | a failing command files `failed/<key>.json` with a non-zero `detail.returncode` -- the launcher's 1 -- with the action's own `detail.action_returncode=7` beside it, and a stderr tail |
| 5 | `--timeout-s` becomes `--time`, and SLURM -- not `pbrun` -- kills the job, which arrives as `detail.slurm.state=TIMEOUT` with `signal=15`, and which `pbrun` reports as `failed (TIMEOUT)` |
| 6 | `--withdraw` on a running job `scancel`s it and files one `withdrawn/` record carrying `withdrawn_by` and the job's detail; nothing lands in `failed/` |
| 7a | `--gres=shard:1` schedules two jobs on a two-shard node and holds the third |
| 7b | `--constraint` for a Feature no node has is refused at submit and reported by `pbrun` |
| 8 | the Epilog ran for a killed job, matched containers by the action's ownership label, and removed its state file as the job's user rather than as root |
| 9 | with no `slurmdbd`, `sacct` answers nothing and the lane's provenance comes from `scontrol` |
| 10a | a three-row `pbcampaign` manifest with mixed demand -- two `shard:1` rows and one no-GPU row -- runs on the fleet and reports one table with each row's job id and node |
| 10b | the same manifest re-run is three CAS hits: no new job id, no new submission record, and no action ran again |
| 10 | from inside a batch step, `scontrol show job` and `scontrol show node` return `Features=` and `ActiveFeatures=` to the job's owner, and `SLURM_JOB_CONSTRAINTS` is unset |
| 11 | `pbrun --measurement --host-class gb10` executes, and the receipt's producer carries `host_class="gb10"` with the controller's `job_features` and `node_active_features` |
| 12 | `--host-class` for a Feature no node has is refused at submit by `sbatch` |
| 13 | `sstat` answers without `slurmdbd` and lists the fields the lane asks for; a job that sleeps with no output is reported by `pbrun` as stalled ("still running"), is not cancelled, and files `done/<key>.json` with `status=executed` and its samples under `detail.liveness`; `liveness.jsonl` in the lane directory holds them |

## What it does not establish

The container is not the fleet, and four things stay open for the install:

- **Device containment.** `ConstrainDevices` is off here and the node's GRES is
  bound to a character device nothing opens. On cgroup v2 the containment is an
  eBPF program that denies exactly the GRES `File=` devices a job was not
  allocated and admits everything else, and whether it lets a job holding
  `shard:1` initialize CUDA while a job holding nothing cannot open
  `/dev/nvidia0` is answerable only on a box with a GPU.
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

Both pass rows 1 to 9. The campaign rows (10a, 10b), the host-class rows
(10 to 12) and the liveness row (13) were added afterwards and have run on
25.11.2 only (run-20260905T010019, 13/13; run-20260905T010156, 14/14; row 13
in run-20260905T010418, before the renumbering). Row 13 adds about five
minutes: a job has to sleep through the lane's 120 s stall window and then
finish on its own. Two things had
to be worked around for 23.11.4, and neither is a lane defect:

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
