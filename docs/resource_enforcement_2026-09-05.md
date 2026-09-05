# Resource enforcement at the cutover: a decision for Rob

Under the pull queue, `pbrun --cpus` and `--demand mem_gb=N` were declarations
that nothing enforced. `pool.py` weighed them at admission and no worker ever
held a job to them: an action that declared `mem_gb=4` and used 40 GiB got 40
GiB, and an action that declared one core and ran `pytest -n 24` got 24 cores.

Under SLURM they become `--cpus-per-task` and `--mem`
(`src/prismabuild/slurm_lane.py:793`), and `fleet/slurm/cgroup.conf`'s
`ConstrainCores=yes` and `ConstrainRAMSpace=yes` turn those into a cpuset and a
`memory.max`. A `pytest -n 24` submitted without `--cpus` then runs on one
core, and a build that outgrows its declared `mem_gb` is held to it -- which,
on this fleet's settings, means throttled into swap rather than killed. The
rows below measure both.

The deadline default was already settled the other way: `pbrun --timeout-s`
defaults to `None`, the partitions are `MaxTime=UNLIMITED`, and an action that
is still progressing is never killed on elapsed time. Cores and memory cannot
be settled the same way. Under `select/cons_tres` with `CR_Core_Memory` there
is no "unset means unlimited": every job consumes a core count and a memory
figure from the node it lands on, and a job that declines to name them takes
the whole node.

So Rob chooses between three settings of `fleet/slurm/cgroup.conf`:

- **Option A. Enforce both.** `ConstrainCores=yes`, `ConstrainRAMSpace=yes` --
  the file as it stands. The declared demand is a contract: an under-declared
  `--cpus` makes the job slow, and an under-declared `mem_gb` holds it to a
  `memory.max`, which as the file stands means throttled into swap rather than
  killed. `ConstrainSwapSpace=yes` is what makes it a kill the platform
  reports; the rows below measure both, and the recommendation returns to it.
- **Option B. Enforce memory, not cores.** `ConstrainCores=no` in
  `fleet/slurm/cgroup.conf`, and `task/affinity` out of `TaskPlugin` in
  `fleet/slurm/slurm.conf` -- two files, because the plugin is what writes the
  cpuset. Cores stay an admission count, as they were under the pull queue.
- **Option C. Enforce neither.** `ConstrainCores=no`,
  `ConstrainRAMSpace=no`. Admission only, which is the pull queue's exact
  semantics.

This document is the measurements, what each option does to three real
submissions, and a recommendation. The decision is Rob's; nothing in
`fleet/slurm/cgroup.conf` or in the `pbrun` defaults was changed to write it.

## What admission does under every option

Admission is not part of the choice. `select/cons_tres` with `CR_Core_Memory`
treats cores and memory as consumable resources whichever way the cgroup
settings go, so under all three options:

- A job is placed only where its `--cpus-per-task` and `--mem` both fit, and a
  node's free memory is reduced by `--mem` while it runs.
- A demand no node can satisfy is refused at submit rather than queued
  forever. The fleet's budgets are `CPUs=20 RealMemory=73728` on sparky,
  `CPUs=20 RealMemory=81920` on sparklina, and `CPUs=80 RealMemory=61440` on
  dl380g10 (`fleet/slurm/slurm.conf:173-198`).

What the three options change is only what happens to a job *after* it is
placed.

## The measurements

Rows 14a-14d of `fleet/slurm/smoke` run a real `slurmctld` and `slurmd` in a
container on a 20-CPU node, with the fleet's scheduler configuration. Three
arms: the file as it stands, option B, and option A with
`ConstrainSwapSpace=yes`. Option C was not run, because its containment is the
absence of what arms 1 and 2 measure and its admission is identical to both.
The rows are quoted verbatim, each from a run whose whole 21-row transcript
passed.

### Option A: `ConstrainCores=yes`, `ConstrainSwapSpace=no` (the file as it stands)

```
[PASS] 14a what a --cpus 1 job may run on [cores=yes swap=no]  job affinity width=1 of the node's 20 CPUs (declared 1); nproc said 4 (OMP_NUM_THREADS, not the cpuset)
[PASS] 14b what a --cpus 2 job may run on [cores=yes swap=no]  job affinity width=2 of the node's 20 CPUs (declared 2); nproc said 4 (OMP_NUM_THREADS, not the cpuset)
[PASS] 14c a job over its mem_gb is constrained, not ignored [cores=yes swap=no]  job=18 declared mem_gb=1, wrote 3072 MiB -> filed done/ state='COMPLETED' rc=0 signal=0 pbrun said failed(COMPLETED)=False and named the declaration=False; binding cgroup /sys/fs/cgroup/system.slice/pbsmoke_slurmstepd.scope/job_18/step_batch/user memory.max=1073741824 (declared 1073741824) memory.swap.max=max memory.swap.current=2246758400 memory.events=[low 0 high 0 max 0 oom 0 oom_kill 0 oom_group_kill 0]
[PASS] 14d a job within its mem_gb completes [cores=yes swap=no]  job=19 declared mem_gb=2, wrote 256 MiB -> filed done/ status=executed state=COMPLETED rc=0
```

### Option B: `ConstrainCores=no`, `task/affinity` dropped

```
[PASS] 14a what a --cpus 1 job may run on [cores=no swap=no]  job affinity width=20 of the node's 20 CPUs (declared 1); nproc said 4 (OMP_NUM_THREADS, not the cpuset)
[PASS] 14b what a --cpus 2 job may run on [cores=no swap=no]  job affinity width=20 of the node's 20 CPUs (declared 2); nproc said 4 (OMP_NUM_THREADS, not the cpuset)
[PASS] 14c a job over its mem_gb is constrained, not ignored [cores=no swap=no]  job=18 declared mem_gb=1, wrote 3072 MiB -> filed done/ state='COMPLETED' rc=0 signal=0 pbrun said failed(COMPLETED)=False and named the declaration=False; binding cgroup /sys/fs/cgroup/system.slice/pbsmoke_slurmstepd.scope/job_18/step_batch/user memory.max=1073741824 (declared 1073741824) memory.swap.max=max memory.swap.current=2246651904 memory.events=[low 0 high 0 max 0 oom 0 oom_kill 0 oom_group_kill 0]
[PASS] 14d a job within its mem_gb completes [cores=no swap=no]  job=19 declared mem_gb=2, wrote 256 MiB -> filed done/ status=executed state=COMPLETED rc=0
```

### Memory that kills: `ConstrainCores=yes`, `ConstrainSwapSpace=yes`

```
[PASS] 14a what a --cpus 1 job may run on [cores=yes swap=yes]  job affinity width=1 of the node's 20 CPUs (declared 1); nproc said 4 (OMP_NUM_THREADS, not the cpuset)
[PASS] 14b what a --cpus 2 job may run on [cores=yes swap=yes]  job affinity width=2 of the node's 20 CPUs (declared 2); nproc said 4 (OMP_NUM_THREADS, not the cpuset)
[PASS] 14c a job over its mem_gb is constrained, not ignored [cores=yes swap=yes]  job=18 declared mem_gb=1, wrote 3072 MiB -> filed failed/ state='OUT_OF_MEMORY' rc=-125 signal=125 pbrun said failed(OUT_OF_MEMORY)=True and named the declaration=True; binding cgroup (none reported) memory.max=- (declared 1073741824) memory.swap.max=- memory.swap.current=- memory.events=[]
[PASS] 14d a job within its mem_gb completes [cores=yes swap=yes]  job=19 declared mem_gb=2, wrote 256 MiB -> filed done/ status=executed state=COMPLETED rc=0
```

### What the rows say

**Cores are enforced exactly, and only under `ConstrainCores=yes`.** A
`--cpus 1` job on a 20-CPU node is confined to 1 CPU and a `--cpus 2` job to 2.
With `ConstrainCores=no` the same jobs are placed against their declaration and
then run on all 20. The declaration is real in both arms; only containment
differs.

**`nproc` is not how a job learns its allocation.** It answered 4 in every arm,
including the one where the job held a single CPU, because `nproc` honours
`OMP_NUM_THREADS` and `pbrun`'s sealed environment pins that at 4
(`tools/fleet/pbrun.py:2577`). An action that sizes its own parallelism from
`nproc` reads that 4 whatever it declared. The figure that matched the cpuset
in every arm was the one the rows read, `taskset -cp $$`; in Python, the same
answer comes from `len(os.sched_getaffinity(0))`.

**Memory is enforced, but on this fleet's settings it throttles rather than
kills.** A job declaring `mem_gb=1` and writing 3072 MiB ran under a
`memory.max` of exactly 1073741824 bytes -- the declaration, to the byte -- on
the cgroup `job_<id>/step_batch/user`. It did not die. `ConstrainSwapSpace=no`
leaves `memory.swap.max` unlimited, so the excess was reclaimed into swap:
2244640768 bytes of it, measured. The job completed, slowly.

**With `ConstrainSwapSpace=yes` the same job is killed.** The row records
`state='OUT_OF_MEMORY' rc=-125 signal=125`; `pbrun` files the action under
`failed/`, reports `failed (OUT_OF_MEMORY)`, and names the declaration that was
exceeded.

**A job inside its declaration is untouched** in every arm.

**On GB10, the memory limit does not see GPU memory.** Measured on sparky on
2026-09-05, outside the container because the container has no GPU: a
4096 MiB CUDA tensor allocated inside a cgroup with `memory.max` set to
2147483648 bytes succeeded, and the cgroup's `memory.current` rose by 83 MiB.
GPU and host share one physical pool on GB10 and the memory controller does not
charge device allocations to the job's cgroup, which is what
`fleet/slurm/cgroup.conf` already records. For a render, `mem_gb` therefore
bounds host memory and admission, and says nothing about the tensors.

## What each option means for three submissions

### `pbrun --cpus 24 -- pytest -n 24`, and the same without `--cpus`

This is the case the cores question exists for. On 2026-09-04, four
`pytest -n 24` runs each declaring `mem_gb=4` were admitted to one 80-core box
together and it reached load average 371.

| | Declared `--cpus 24` | Declared nothing (`--cpus` defaults to 1) |
|---|---|---|
| A | 24 cores held, 24 cores usable | Runs 24 workers on 1 core. Correct, and slow by the parallelism it did not declare |
| B | 24 cores held, 24 cores usable | 24 workers on all 20-80 cores of the box, holding an admission claim on 1 |
| C | As B | As B |

Admission prevents the load-371 incident under all three options *when the
jobs declare*: four jobs at `--cpus 24` do not fit on 80 cores, and the fourth
queues. The incident's jobs did not declare. At the `--cpus` default of 1, all
four are admitted under every option, and only option A keeps the box out of
trouble anyway, by holding each run to the one core it asked for. That is the
difference the measurements draw: under A an under-declared job hurts only
itself, and under B and C it hurts every co-tenant, which is what happened on
2026-09-04.

**Cores have no failure state.** An under-declared `mem_gb` at least leaves a
mark -- `OUT_OF_MEMORY` under `ConstrainSwapSpace=yes`, swap the job did not
ask for without it. An under-declared `--cpus` ends `COMPLETED`, slowly, and
nothing in the record distinguishes a core-starved job from a slow one. Under
option A, the submit line is the only place the platform says what the cpuset
will be, which is why `demand=` carries `cpu`.

### A 27B render declaring `--demand mem_gb=90`

Refused at submit under all three options, and the option choice has nothing to
do with it: `--mem=92160M` exceeds every node's `RealMemory`, the largest of
which is 81920 MiB on sparklina. `sbatch` refuses a job no node can ever
satisfy, and `pbrun` reports `slurm refused this action` with the demand and the
message SLURM wrote. This one is read from the fleet's node budgets and SLURM's
documented behaviour; the smoke container's node is 16 GiB, so the refusal was
not measured on a 90 GiB demand here.

A render that declares a figure that fits -- `mem_gb=60`, say -- behaves
differently in a way that matters more:

| | Behaviour |
|---|---|
| A, B | 60 GiB `memory.max` on host memory. CUDA allocations are not charged to it, so a render whose host footprint stays under 60 GiB is unaffected however large its tensors are. A host-memory spike above 60 GiB is throttled into swap under `ConstrainSwapSpace=no`, or killed with `ConstrainSwapSpace=yes` |
| C | No limit. The declaration is an admission claim only |

Under A and B, `retry_safe` actions resubmit after `OUT_OF_MEMORY`
(`slurm_lane.py:198`) with an identical `--mem`, so a retried OOM dies the same
way, once per remaining attempt.

### `pbrun -- echo hello`

| | Behaviour |
|---|---|
| A | 1 core, 4 GiB. Both are ample; the job is unaffected |
| B | 4 GiB, all cores visible |
| C | Unconstrained |

Nothing in this shape is at risk under any option. It is worth stating because
it is most of what the fleet runs, and the argument for A cannot rest on jobs
that would never notice it.

## Recommendation

**Take option A.** The measurements decide it in two steps. First, the case
that motivates enforcement is the under-declared job, and that is exactly the
case B and C do not cover: admission holds an over-declared `pytest -n 24` in
check under every option, but the four runs that took a box to load 371 declared
nothing, and at the `--cpus` default of 1 only option A confines them.
Second, the cost that would argue against A is smaller than it looks, because
on this fleet's settings memory does not kill: an under-declared job
reclaims into swap and finishes, so a wrong `mem_gb` on a host-memory spike is
a slowdown rather than a lost build, and GPU memory -- the dominant consumer in
every render -- is not charged to the limit at all. That leaves under-declared
cores as A's one real cost, and it is a cost with a fix the submitter controls
and a message at submit time that names the number.

Taking A leaves one sub-choice, and it is a real one. Option A is usually
described as "slow or OOM-killed, and the platform says so plainly", and the
rows show that the file as it stands does not do the second half: with
`ConstrainSwapSpace=no` an over-declared job throttles into swap and reports
`COMPLETED`, so the platform says nothing at all. `ConstrainSwapSpace=yes` is
what turns the limit into the plain refusal, at the price of ending a build that
would otherwise have finished slowly. Both behaviours are measured above, in
arm 1 and arm 3. This document does not pick between them, because the choice
turns on which of a lost build and a silent slowdown Rob would rather debug.

Two further things follow from taking A, and neither is a reason to take B:

- The `--cpus` default of 1 is the sharp edge, not `ConstrainCores`. Anything
  parallel has to declare, and nothing at exit will tell a caller that it did
  not. Watch for the first slow suite, and treat it as a missing declaration
  before treating it as a slow suite.
- If the swap sub-choice goes to `yes`, `retry_safe` actions resubmit after
  `OUT_OF_MEMORY` with an identical `--mem`
  (`slurm_lane.py:198`), so an OOM under A burns every remaining attempt on the
  same limit before it is reported.

## Reproducing the measurements

```bash
DEB_DIR=/home/rob/slurm-build/arm64-24.04 bash fleet/slurm/smoke/run.sh
PB_SMOKE_CONSTRAIN_CORES=no DEB_DIR=... bash fleet/slurm/smoke/run.sh
PB_SMOKE_CONSTRAIN_SWAP=yes DEB_DIR=... bash fleet/slurm/smoke/run.sh
```

Each run prints the arm it is in at the top of its transcript and names it in
every row. The rows quoted above come from
`/home/rob/slurm-build/smoke/run-20260905T022428` (arm 1),
`run-20260905T020326` (arm 2) and `run-20260905T020952` (arm 3), each 21/21.
`fleet/slurm/smoke/README.md` has what the harness does and does not
establish.
