# Operating PrismaBuild

This guide is for the operator or agent who puts work on the fleet. It covers
submitting a command, waiting for it, running a campaign, watching what the
fleet is doing, stopping work, and reading a failure.

To install SLURM, see the [SLURM install
runbook](slurm_runbook_2026-09-04.md). For why the fleet moved from its own pull
queue to SLURM, see the [scheduler decision](scheduler_decision_2026-09-04.md).
For the system's own map, see [the design document](design.md).

Two dispatchers carry work: the pull queue (`pool`) and SLURM (`slurm`). The
result does not depend on which one carried it. Examples below name the
transport explicitly with `--transport slurm` where SLURM behaviour is the
point. You can set `PRISMABUILD_TRANSPORT=slurm` instead, and the published
runtime generation carries a default that applies when neither is set. See
"Publish the runtime the fleet executes" for what a generation is and who
may publish one.

## Actions, keys, and why a re-run is free

An action is one command sealed into a content-addressed record. `pbrun`
computes an action key over the command, the code closure, the parameters, the
environment that matters, and the effective placement. The key is a 64-hex
digest, and the fleet prints its first 12 characters.

Three properties follow from that.

*   **A snapshot, not a path.** `pbrun` seals your Git checkout — dirty and
    untracked bytes included — as a Git bundle in the content-addressed store
    (CAS). The worker materializes a fresh checkout of that commit wherever the
    action lands, so `HEAD~1`, `git merge-base` and `BASE...HEAD` resolve there.
    Edits you make after submitting cannot change what runs. A path you staged
    with `git add -f` travels too, with the bytes it has in your worktree, even
    though the ignore rules match it; a path your worktree no longer has does
    not travel, whether it was committed or only staged.
*   **A receipt is the verdict.** A worker that finishes the work publishes a
    CAS receipt. Under SLURM, a job that exits 0 without publishing a receipt
    did not do the work, and a job that ends badly after publishing one did.
*   **A re-run is a lookup.** Asking for the same work again produces the same
    key. If the CAS already holds a receipt for it, nothing runs and the
    submission reports `cache_hit`. That is what makes a campaign resumable:
    submit every row, and only the missing ones cost anything.

Placement is part of identity. `pbrun` normalizes and sorts the tags that
landed, including a hostname pin derived from a box-local executable, and seals
them before computing the key. Flag order and duplicate tags do not move the
key; a different admissible worker population does.

## Submit one command

The wrapper runs without login profiles. Use an absolute executable or seal
its required PATH with `--env PATH=...`; a worker's shell startup files are
not dependencies. Native thread defaults match `--cpus` (or `cpu=` in
`--demand`), and explicit `--env` overrides are preserved. Reserve the total
cores and memory used by parallel child processes, including pytest workers.

Submit a command with `pbrun`. Everything after `--` is the command.

    tools/fleet/pbrun.py --gpu --timeout-s 3600 -- ./stage.sh --shard 3

`pbrun` waits for the action and exits with the result. `--cwd` selects the
checkout to seal; it defaults to the current directory and must be inside a Git
checkout on the box you submit from. The checkout must be writable: `pbrun`
keeps its closure stamp there, and the action tees its output to a result file
in the same tree.

The checkout has a size ceiling. `pbrun` refuses a working tree whose sealed
paths exceed 512 MiB, before it hashes anything, and refuses the bundle at the
same bound. `--checkout-snapshot-max-bytes N` lowers that ceiling for one
submission, which is useful when you want the refusal early rather than after a
large tree has been read. It cannot raise it: a value above the fleet ceiling is
refused by name. The ceiling covers local disk on the box that materializes the
checkout, so it is not part of the action's identity.

### Placement vocabulary

These flags say what the action needs and where it may run.

| Flag | What it means | What SLURM gets |
|---|---|---|
| `--gpu` | Shorthand for `gpu=1,mem_gb=16`. | `--gres=shard:1`, partition `gpu`. |
| `--demand gpu=2,cpu=8,mem_gb=32` | The full demand. `mem_gb` defaults to 4, `cpu` to `--cpus`. | `--gres=shard:2 --cpus-per-task=8 --mem=32768M`. |
| `--exclusive` | The whole GPU of one box. Requires a GPU demand. | `--gres=gpu:1` rather than a larger shard count. |
| `--gpu-capacity N` | Slots to demand for `--exclusive`. | Under SLURM, only `1` is accepted: `--gres=gpu:1` is the whole device, so a larger count would be read and discarded. |
| `--cpus N` | Cores the action will actually use. Defaults to 1. | `--cpus-per-task=N`. |
| `--tag NAME` | Require a box offering this tag. Repeatable. | `--constraint=NAME`, ANDed with `&`. |
| `--here` | Pin the action to this box. Combines with `--tag`. | The box's hostname joins the constraint. Every hostname is a node Feature. |
| `--anywhere` | Assert that dependencies outside the snapshot are identical on every eligible worker. | No constraint, and the default partition. |
| `--priority N` | A queue hint. Higher runs sooner. Defaults to 0. | `--nice`, sent on every submission. SLURM subtracts the nice from the base priority its scheduler assigned. |

`--tag` and `--here` are two constraints, and passing both applies both:
`--here --tag gb10` places the action on this box, which must also offer the
`gb10` tag. The tags are sorted before they are sealed, so the order you pass
them in does not move the action key.

`--anywhere` contradicts both of them, and `pbrun` refuses each pairing.
`--anywhere --here` names one box and calls the action portable. `--anywhere
--tag gb10` does the same with a class: the assertion is that every eligible
worker can run the action, and the tag admits only the boxes offering it. Drop
whichever is not true.

`--priority` is a queue hint and nothing more. It is not part of the action
identity, so two submissions that differ only in priority are the same action.

### Which partition an action lands in

The lane derives the partition from the demand and the placement, so it adds
nothing to the action's identity:

*   A GPU demand goes to the `gpu` partition, the only place shards exist.
*   `--anywhere` on CPU-only work goes to the default partition `all`, where
    node weight prefers dl380g10 and a GB10 box takes it only when dl380g10 is
    full.
*   CPU-only work with no tag goes to the `cpu` partition.
*   Tagged work goes to the default partition, where the sealed `--constraint`
    picks the node.

### What every submission sends

Every job is submitted with `--no-requeue`, `--export=NIL` and
`--dependency=singleton`, under the job name `pb-<first 12 characters of the
key>` and with `--comment=pb:<key>:<attempt>:<nonce>`. Only SLURM's own variables reach the job; the action's environment is
the sealed one the worker builds. `--chdir`, `--output` and `--error` point at
the action's own lane directory. Retries are new submissions with new job ids,
never `--requeue`. The [install runbook](slurm_runbook_2026-09-04.md) shows a
full `sbatch` line.

### One job per action key at a time

SLURM scopes `--dependency=singleton` by job name and user, and the job name is
the action key. So the controller runs one job of a key at a time and holds the
rest.

That is what closes a window the submitter cannot. `pbrun` asks the CAS before
it submits, and it attaches to a submission that is still running rather than
starting a second copy. Two `pbrun`s that look at the same instant both see
nothing and both submit. The scheduler orders them.

A held job is `PENDING` with reason `Dependency`. Nothing is stuck, nothing is
refused, and nothing is cancelled.

*   `pbstatus` prints the reason and names the job ahead: `waiting for job 1001
    of the same action`.
*   `pbrun` and `pbwait` say the same thing on stderr while they wait, and
    repeat it at most every five minutes.
*   When the job ahead leaves, the held job starts, finds the receipt it
    published, and exits without materializing a checkout. Its ending is
    `cache_hit`.

A held job costs a job id and a node slot for as long as it takes to read one
receipt. It does not cost a checkout or a second execution.

### Demand is enforced under SLURM

On the pull queue, a demand decided what could be placed and nothing stopped an
action from using more. On 2026-09-04 four `pytest -n 24` runs each declaring
`mem_gb=4` were admitted to one 80-core box together, and its load average
reached 371.

Under SLURM the same demand becomes `--cpus-per-task` and `--mem`, and
`cgroup.conf` contains cores, memory, and devices. Declare what the work uses.

## Wait, detach, and give up

By default `pbrun` waits. `--wait-s` bounds how long it waits and defaults to
86400 seconds. It bounds only your patience: nothing is cancelled when it
expires, and the job keeps running.

To submit without waiting, use `--detach`:

    tools/fleet/pbrun.py --detach --gpu -- ./stage.sh --shard 3

`--detach` prints one JSON line naming the action key, the transport, the job
id or queue record, the generation, and the paths the ending will be filed at,
then exits 0. An action already in the CAS prints `status=cache_hit` and
submits nothing. One already running prints `status=attached` and joins that
run rather than starting a second copy of it. `--detach` refuses
`--max-attempts` greater than 1, because a retry needs somebody alive to see
the first attempt fail.

Wait for detached keys later, in any number, with `pbwait`:

    tools/fleet/pbwait.py 8fc86da0e13f 4b19a02cc551

`pbwait` accepts a full key or a prefix that something has already recorded. It
prints one table and exits 0 only if every action's work is done. Its `--wait-s`
is the deadline for all the keys together, not for each. If the submission it
recorded has since been superseded by a newer submission of the same key, it
waits out `--wait-s` and exits 75 rather than file an ending for the wrong
generation; run `pbwait` again and it reads the newer one.

Under SLURM, `pbwait` does more than watch. The waiting process files the
terminal record, so a detached submission has nobody to file one. `pbwait`
resumes the recorded job, waits on it, and files that ending. Under the pull
queue the worker files the ending and `pbwait` only watches.

A full key may name work that is not submitted yet, and waiting first is
supported: while nothing is recorded, each poll looks for a submission as well
as for an ending, so a detached submission made after the wait began is
discovered, resumed, and reported by that same wait. Before this, such a wait
watched only terminal files, spent its whole `--wait-s` on a job that had
already finished, and exited 75.

### Exit codes

`pbrun` and `pbwait` use the same codes.

| Code | Meaning |
|---|---|
| 0 | The work is done. A `cache_hit` counts as done. |
| 1 | The action failed. `pbrun` prints the worker's message and the log paths. `pbwait` also exits 1 when an ending was filed and cannot be read, and names the file: that is not 75, because waiting again only re-reads the same record. |
| 2 | `pbrun --withdraw` matched no submission, matched more than one, or every `scancel` refused. `pbwait` was given a key that is empty, that matches no record, or that matches more than one. Also argparse's own usage error. |
| 74 | SLURM took the action, but `pbrun` could not write the record of it. `sysexits.h` calls 74 `EX_IOERR`, and that is what happened: the job is real and the work may be finished, only the account of it failed. |
| 75 | No verdict yet. The wait ended before the work did, or `sbatch` stopped answering and the controller could not say whether it took the job. Nothing was cancelled and nothing was filed. |
| 143 | The action was withdrawn. 128 + SIGTERM, the signal a withdrawal sends. |

Exit 1 is the worker launcher's status, not the command's own exit code. A
command that exits 7 makes the worker refuse to publish a receipt, and both
transports report that refusal as 1. Under SLURM the terminal record also
carries the command's own status, as `detail.action_returncode`, with
`detail.action_signal` beside it when a signal ended the command. `pbrun`,
`pbwait`, and `pbstatus` print that number next to the launcher's. The field is
absent when the ending was the worker's verdict rather than the command's, such
as a missing result file or a timeout. The pull queue's records do not carry
it.

The two transports reach the verdict by different rules, and they part on one
ending. The pull queue's authority is the launcher's exit code; the lane's is
the receipt. A launcher that publishes its receipt and is then signalled is
filed `failed` by the queue and `executed` by the lane.

After a 75 from a wait, run `pbwait` on the key. The job is still queued or
running, and under SLURM `pbwait` is what files the ending once it stops.

After a 75 that says the fate of a submission is unknown, run the `squeue` in
the message instead. There is nothing to wait on: no submission was recorded,
because none is known.

### When the pull queue is fenced

`fleet/slurm/cutover.sh` closes the pull queue to new submissions before it
retires the queue's workers, and it does that with the filesystem rather than
with a flag: it removes the write bit on
`/mnt/shared/prismabuild-fleet/pb-queue/ready` and writes
`pb-queue/cutover-fence.json` beside it saying why. Every producer on the fleet
is still running the generation published before the cutover, and that code
reads no marker, so the write bit is the only thing all of them obey.

A submission into a fenced queue is refused rather than accepted:

    pbrun: the pull queue is fenced: /mnt/shared/prismabuild-fleet/pb-queue/ready
    is not writable. fleet/slurm/cutover.sh is retiring the pull queue's
    execution plane; submit through SLURM, or run fleet/slurm/rollback.sh.

Submit through SLURM instead with `pbrun --transport slurm`, or wait for the
cutover to finish, after which the published generation makes SLURM the
default and nothing has to be said. The fence stays up once the cutover has
finished, because the queue then has no workers.
`fleet/slurm/rollback.sh` lifts it, restoring the mode the cutover recorded in
the marker.

An unwritable `ready` with no marker is not a fence. It is a directory
somebody tightened, and it is refused the same way, naming the directory so
the mode can be read.

### When a record will not write

The lane writes every fact it keeps after the fact is already true: the
submission record after `sbatch` returned an id, the terminal record after the
receipt landed in the CAS. A full mount, a queue directory somebody tightened,
or a stale NFS handle turns that write into an error at a point where the job
is real and the work may be finished.

`pbrun` reports it and exits 74:

    pbrun: slurm took this action, but pbrun could not write its record.
      slurm job: 1743
      record:    /mnt/shared/prismabuild-fleet/pb-queue/done/<key>.json
      reason:    Permission denied
    The receipt is in the CAS, so the work is done and re-running costs nothing.
    Clear what blocked the write, then run `tools/fleet/pbwait.py <key12>` to
    file the ending.

The last two lines change with what the CAS holds. With no receipt, the job may
still be running, so the advice is to `pbwait` on it or to withdraw it. Either
way the job id is on the line, because the record that would have carried it is
the one that failed.

A lane error raised after `sbatch` accepted the job reports the same way, with
the same exit code. Only a refusal with no job behind it reports as a refusal,
and only that one tells you to fix the `--tag`: a job the controller has
already taken is not fixed by changing the submission.

### When `sbatch` stops answering

A scheduler command gets 60 seconds. `sbatch` is the one where that bound sits
in the wrong place: the controller can accept a submission and the client can
then hang, so the job runs with its id lost.

Every invocation of `sbatch` therefore names itself in the job's `Comment`:
`pb:<key>:<attempt>:<nonce>`, a fresh nonce each time, sealed into the
submission record with the rest of the argv. The job name cannot do this --
every attempt of every submission of one key shares it.

On a timeout the lane asks the controller whether it took the job:

    squeue -h -u $USER --name=pb-<key12> --states=all -o '%i|%k'

`--states=all` because a job accepted and finished inside the same 60 seconds
would not be listed otherwise; matching on the comment is what makes the wider
listing safe. One job carrying this invocation's comment is adopted, and the
submission record is written as if `sbatch` had printed that id. No job
carrying it means the controller did not take the submission, and the refusal
stands. A controller that cannot be asked leaves the fate unknown: `pbrun`
prints the job name, the comment and that `squeue`, files nothing, and exits
75.

### No deadline exists by default

`pbrun` sends no `--time` unless you pass `--timeout-s`. A job that is doing
something runs until it ends. Elapsed time is never treated as evidence that a
worker is dead.

When a `--timeout-s` you asked for does expire, the worker takes the action's
whole process group down before it reports the timeout: SIGTERM, a grace
period, then SIGKILL against whatever is still running. A descendant that
ignores SIGTERM does not survive the report.

What the lane does instead is measure. While a job is `RUNNING`, the waiting
`pbrun` samples the job's own cgroup accounting and its log sizes at the
accounting interval. A sample is progressing when CPU time, RSS, disk bytes, or
log size changed. After 120 seconds of unchanged samples, `pbrun` prints one
line to stderr and repeats it every ten minutes:

    pbrun: <key12> slurm job <id> has shown no progress for <N> min on <node>; it is still running. Withdraw with pbrun --withdraw <key12> if it is dead.

It cancels nothing, ever. A stall ends when the job finishes, when you withdraw
it, or when it reaches a `--timeout-s` you asked for. Every sample is appended
to `liveness.jsonl` in the action's lane directory, and the terminal record
carries the last sample under `detail.liveness`. The runbook's liveness section
gives the fields, the cadences, and how the 120-second window was derived.

## Run a campaign

A campaign is a JSON list of independent rows. There is no dependency graph; a
dependency is a later manifest.

    tools/fleet/pbcampaign.py manifest.json

Every row goes through `pbrun`'s own seal path, so a row's action key is the key
a hand-typed `pbrun` produces for the same row. Re-running a manifest is
therefore free for every row that finished.

### Row schema

Every field except `argv` is optional. Each one is exactly one `pbrun` flag, and
an omitted field is not passed at all.

| Field | Flag |
|---|---|
| `argv` | The command, as a list of strings. Required. |
| `cwd` | `--cwd` |
| `demand` | `--demand`, as an object: `{"gpu": 1, "cpu": 8, "mem_gb": 32}` |
| `tags` | `--tag`, once per entry |
| `env` | `--env K=V`, once per pair |
| `timeout_s` | `--timeout-s` |
| `deterministic` | `--deterministic` |
| `anywhere` | `--anywhere` |
| `here` | `--here` |
| `no_default_env` | `--no-default-env` |
| `snapshot_ref` | `--snapshot-ref`, once per entry |
| `exclusive` | `--exclusive` |
| `gpu_capacity` | `--gpu-capacity` |
| `priority` | `--priority` |
| `measurement` | `--measurement` |
| `host_class` | `--host-class`, a node Feature name such as `gb10` |
| `retry_safe` | `--retry-safe` |
| `max_attempts` | `--max-attempts` |

An unknown field is refused when the manifest loads, before any row is sealed:
a dropped typo would seal an action nobody asked for.

Every field's value shape is refused at load too, and for the same reason: a
value that cannot become its flag used to raise while a later row was being
prepared, after the rows before it had been submitted, and their keys went with
the traceback. A count in `demand` is an integer or a string holding one, a
name in `demand` and `env` is a string, an `env` value is a string or a number,
`tags` and `snapshot_ref` are lists of non-empty strings, `timeout_s` is a
number, `cwd` and `host_class` are strings, and every switch field is `true` or
`false` rather than anything truthy. Two of those refusals were silent before:
`"tags": "x86"` sealed three tags, one per character, and `"deterministic":
"no"` sealed the opposite of what it said. Each refusal names the row index,
the field, and the value.

Three rows are refused at load as well, each for the reason `pbrun` gives at
submit:

*   `measurement` without `host_class`. A measurement's numerics do not
    transfer across architectures, so its result is keyed on the class that
    produced it.
*   `host_class` under `--transport pool`. The class is attested through the
    SLURM controller, so a pull-queue worker refuses the action at preflight.
    This is the one refusal that depends on the campaign's transport rather
    than on the row.
*   `max_attempts` greater than 1. A campaign submits every row detached,
    which is what lets one command hold N actions open, and a retry needs
    somebody alive to see the attempt fail.

Set `retry_safe` on a row even without `max_attempts`. The retry policy is
sealed into the action's identity, so a row that omits it is a different action
from the hand-typed `pbrun` that passes it.

A campaign of measurements therefore reads like this, and every row of it is a
cache hit on the second run:

    [
      {
        "argv": ["./probe.sh", "--shard", "0"],
        "cwd": "/home/rob/mypkg",
        "measurement": true,
        "host_class": "gb10",
        "retry_safe": true
      }
    ]

Run it with `--transport slurm`, from a box of that class.

`--transport` is a flag on the campaign, not a row field, because which
dispatcher carries the work is a fact about the fleet. One caveat travels with
it: `exclusive` is the one field whose demand `pbrun` derives differently per
transport, so an exclusive row keyed on one transport is a different action on
the other, and the two do not memoize each other.

Two rows, one wanting a GPU and one that must not have one:

    [
      {
        "argv": ["/home/rob/venv/bin/python", "-m", "mypkg.stage", "--shard", "3"],
        "cwd": "/home/rob/mypkg",
        "demand": {"gpu": 1, "mem_gb": 32},
        "timeout_s": 7200,
        "env": {"PYTHONPATH": "src"}
      },
      {
        "argv": ["/usr/bin/python3", "-m", "pytest", "-q", "tests"],
        "cwd": "/home/rob/mypkg",
        "demand": {"cpu": 8, "mem_gb": 16},
        "tags": ["x86"]
      }
    ]

### Resume a campaign

Run the same manifest again. A row that finished is a cache hit and costs
nothing. A row still on a node is attached to by its recorded job id rather than
started a second time. A waiter that died, a closed laptop, or a dropped
connection therefore costs the wait, never the work. To submit and walk away,
use `--detach`, then `pbwait` on the keys it printed.

### Campaign exit codes

| Code | What it means |
| --- | --- |
| 0 | Every row's work is done. A cache hit counts as done. |
| 1 | A row was refused before submission, a row's work failed, or the manifest did not load. |
| 75 | Nothing failed, and at least one row was still running when `--wait-s` ran out. |

A refusal outranks a failure and a failure outranks a wait, so 75 means the
work is still out there and the keys are still worth waiting on. Under
`--detach` the campaign returns 0, or 1 if any row was refused; it does not
wait, so it never returns 75.

## Fan a test suite out

`pbtest` shards a test suite across the fleet instead of running it on one box:

    tools/fleet/pbtest.py --checkout /home/rob/prismabuild \
        --python /home/rob/venvs/pb-cpu/bin/python --shards 20 tests

Each shard is one `pbrun` action, so the checkout travels through the CAS and
the interpreter is the target box's, not this one's. `--tag` defaults to `x86`,
which is also the claim that owns the named interpreter.

`--threads-per-shard` sets each shard's BLAS and OMP ceiling, and the same
number becomes that shard's `pbrun --cpus`, which the lane emits as
`--cpus-per-task`. The two travel together on purpose: a ceiling without a
reservation is threads taking turns inside one core, because `ConstrainCores`
makes the declared demand a cpuset. `--cpus-per-shard N` reserves a different
number, and it is required with `--threads-per-shard 0`, which sets no ceiling
and so gives nothing to derive a reservation from. A negative ceiling, a reservation below one core, and a missing
pairing are all refused with exit 2 before any shard is submitted.

The CPU demand is sealed into each shard's action, so a suite fanned out at a
different width is a different action rather than a cache hit of the last run.

## Submit a measurement

A measurement's numerics do not transfer across architectures, so a measurement
is keyed on the host class that produced it:

    tools/fleet/pbrun.py --transport slurm --measurement --host-class gb10 -- ./probe.sh

`--host-class CLASS` names a node Feature. It seals `execution_scope
host_class_keyed`, joins the effective placement so the action key moves with
it, and the SLURM lane sends it as `--constraint=CLASS`.

Three constraints follow, and each is enforced rather than advised:

*   **`--measurement` refuses without `--host-class`.** A portable measurement
    would let any box's KL stand in for another's.
*   **`--host-class` refuses without `--transport slurm`.** The class is
    attested through the SLURM controller, so a pull-queue worker refuses the
    action at preflight.
*   **Submit from a box of that class.** A host-class-keyed action is
    nonportable, so it binds the submitting box's `argv[0]` digest and its ABI
    and accelerator facts. A worker of another class refuses it at preflight,
    naming the field that differs.

The worker, not the submitter, attests the class. It reads `scontrol show job`
for the partition, batch host and the job's own constraint, then `scontrol show
node` for that node's active features. The class is attested when the node
carries the Feature and the job's constraint is a plain conjunction requiring
it, so the scheduler enforced the placement rather than a worker observing it.
An unreachable controller refuses. The controller's answer is recorded on the
receipt under `evidence.slurm.controller`.

## Watch the fleet

`pbstatus` answers the three questions in one command, with no arguments:

    tools/fleet/pbstatus.py

It prints three tables:

*   **nodes** — what the controller can reach, with partitions, state, allocated and
    idle CPUs, memory, GRES in use out of GRES declared, features, load, and a flag naming why any node is
    not schedulable.
*   **jobs** — what is queued or running, joined against the lane's submission
    records so each row also carries the action key prefix, the resources the
    action asked for, the constraint it was placed under, and the box that
    submitted it.
*   **endings** — how the last actions ended, newest first, each labelled with
    the transport that produced it. `--recent N` changes how many are read; the
    default is 20.

`pbstatus` never blocks, never writes, and never fails. A controller that is not
installed prints one line saying so, and the endings table still prints, because
those records are files on the shared mount. A record it cannot read prints as an
`unreadable` row whose note names the path and the reason, so a truncated or
unreadable newest record does not read as a fleet that filed nothing. `--json`
prints one object with the three lists and any scheduler notes.

Two flags say where `pbstatus` looks. `--lane-root` is the SLURM lane root that
job names are resolved against, and it defaults to `$PRISMABUILD_SLURM_LANE_ROOT`, or
to the fleet lane root when that is unset. `--queue-root` is the queue root
holding `done/` and `failed/`, which is where the endings table is read from.
Point them at a test fleet to read one without touching the live store.

The underlying commands are `sinfo` for nodes, `squeue` for jobs, and `sacct`
for jobs the controller has forgotten. Use them directly for scheduler detail
`pbstatus` does not join in.

### Where the records live

| Location | What is there |
|---|---|
| `/mnt/shared/prismabuild-fleet/pb-queue/done/<key>.json` | The ending of an action whose work was done. |
| `.../pb-queue/failed/<key>.json` | The ending of an action with no receipt. |
| `.../pb-queue/withdrawn/<key>.json` | The marker for an action somebody cancelled. |
| `.../pb-queue/claimed/<key>.<unix>.<host>.<pid>.<id>.tombstone` | A claim moved aside while its finisher publishes the action's next home. It exists for one write, and the finisher deletes it. One that outlives the lease timeout means the finisher was interrupted: the next reaper puts the record back as a claim, or, if the key already has a live or terminal record, files it under `withdrawn/superseded/` as evidence. No reader of `claimed/` counts it as a claim. |
| `.../slurm/<key>/` | The lane directory: `scripts/<sha256>.sh`, the immutable script each submission sent, plus `job.sh` as a pointer to the newest, `submissions/`, `latest.json`, `liveness.jsonl`, and `<jobid>.out` and `.err`. |
| `.../cas/` | The content-addressed store: action requests, results, and receipts. |

Both transports file their endings in the same two directories, so a SLURM
ending and a pull-queue ending appear side by side. The `schema` field says
which filed it: the two writers use distinct schema ids, and only the lane
writes a `transport` field. `pbstatus` labels its endings table from the schema
for that reason.

A job's node-side cleanup is the Epilog's, and it reads what to clean out of a
state file under `.../slurm/jobs/`. When that root is unreadable, which is what
a shared-mount outage looks like from a compute node, the Epilog logs one line
naming the root and falls back to removing containers labelled with the SLURM
job id alone. Materialized checkouts are not removed on that path, because the
tree to remove is only ever the one the state file records. After an outage,
grep `slurmd.log` for `could not be read`, then look under
`/home/rob/tmp/prismabuild-checkouts` for trees the Epilog could not name.

### Read a terminal record

A terminal record's top level carries `status`, `action_key`, `transport`,
`attempts`, `max_attempts`, `resources`, `tags`, `claimed_host`,
`finished_host`, `finished_unix`, and `detail`.

Inside `detail`: `status` and `returncode` in the pull queue's own terms,
`signal`, `elapsed_s`, `receipt_published`, `result_digest`, and the last 256
KiB of each stream in `stdout` and `stderr`. Under SLURM, `detail.slurm` carries
the job id, the scheduler state, the partition, and the paths of the submission
record and the full logs. `detail.liveness` carries the last liveness sample.

Read `status` before `returncode`. A job SLURM killed at its time limit reports
`ExitCode=0:15` — exit code zero, signal fifteen — so the lane files it as
`status: "timeout"` with `returncode: null`, and a reader that takes zero as
success is not misled.

## Stop work and retry it

### Withdraw

Withdrawing cancels a run. It is the only way to take work off the fleet:

    tools/fleet/pbrun.py --transport slurm --withdraw 8fc86da0e13f --reason "superseded by the 4.75 arm"

A key prefix is enough, and `--withdraw` is repeatable. An ambiguous prefix is
refused rather than guessed, and one bad name does not stop the others.

You do not have to name the transport. A SLURM job has a submission record under
the lane root and a pull-queue item has none, so the record decides where each
prefix goes. `--transport slurm` sends every prefix to the lane.

Under SLURM, a withdrawal writes the marker and then the terminal record, both
under `withdrawn/`, before it runs `scancel`. Nothing lands in `failed/`: a
withdrawal is a decision, not a failure. That order matters: `pool_reset` skips
a re-submission only on a marker or a `withdrawn_unix`, so a cancellation with
neither would be re-submitted by the next bulk reset. If `scancel` is refused,
run the same command again: the decision on disk is kept, and `scancel` is
retried until it accepts the job.

A withdrawal cancels every job the controller holds under the key's name, and
names each id it cancelled. That is more than the one job `latest.json`
records, because it has to be: two submissions of one key that raced each other
leave a second job `PENDING` on `Dependency`, and cancelling only the recorded
id leaves that one to run the withdrawn action when the singleton releases it.
The listing is scoped to your own user, so a second person's job of the same
action is not touched. A `squeue` the controller does not answer costs the
siblings and not the cancellation: the recorded id is always cancelled. Exit
status is 2 only when every `scancel` was refused; one refusal among several is
reported and the withdrawal stands, because a sibling that finished between the
listing and the cancel is the ordinary case.

A withdrawal cancels the run, not the name. The marker is scoped to the
generation it was filed against, and a submission of a *later* generation
retires it into `withdrawn/superseded/`: the action key is a content hash, so
re-submitting it is how anybody asks for the same work again. A withdrawal of
the run being submitted is left where it is, whenever it lands: it stops the
remaining attempts of that run and it is the ending that gets filed. If the
action finished a moment before you asked, `pbrun` says an outcome is already
filed and withdraws nothing.

On the pull queue, a withdrawal returns the action's capacity only once the
action is known to have stopped. That is one of three things: the signal ladder
on the holder's own box reported the process group dead, the holder is the box
you ran the command on and no process there owns the action, or the holder's
lease stopped beating and the reaper concluded the claim. Otherwise `pbrun` says
`release pending on <host>` and reports `released 0`, and the claim, lease and
reservation stay where they are with a `stop_pending` object stamped on the
claimed record. The holder's worker sees the marker within a heartbeat, stops
the action, files it under `withdrawn/` and returns the tokens then. Read
`released 0` as "not yet", not as "there was nothing to release": a cross-box
withdrawal is the ordinary case and it always reads that way. Waiting is the
point. Releasing on the operator's word alone let a replacement action be
admitted on the holder's only CPU token while the original payload was still
running.

### Retry

An arbitrary command gets one attempt. Numerical determinism says the result
bytes repeat; it does not make external effects idempotent, and a command can
write state outside its declared result before a later gate fails.

To opt into bounded retry, declare the whole command idempotent:

    tools/fleet/pbrun.py --retry-safe --max-attempts 3 -- ./stage.sh --shard 3

`--max-attempts` greater than 1 without `--retry-safe` is refused, and
`--deterministic` does not substitute for it: it covers result bytes, not
external side effects. The policy is sealed into the action's identity and
carried in the record.

Under SLURM a retry is a new submission with a new job id. It is attempted only
after `BOOT_FAIL`, `FAILED`, `NODE_FAIL`, `OUT_OF_MEMORY`, `PREEMPTED`, or
`TIMEOUT`. `CANCELLED` is excluded because it is somebody's decision, and
`DEADLINE` because a resubmission would meet the same absolute deadline
immediately.

### Reset a batch of failures

`pool_reset` re-submits the queue's failed items. It resets the work, not the
record. For an action addressed by a path, which is what the pull queue files,
it recovers the command, working directory and demand from the CAS request and
submits again through `pbrun`, which re-seals the closure against the tree as
it is now.

    tools/fleet/pool_reset.py                 # report only
    tools/fleet/pool_reset.py --apply --limit 20

Either invocation works from a checkout and from a published runtime
generation: the child `pbrun.py` is looked up under both layouts, `tools/fleet`
first and then the published flat `tools`, which hold the same bytes. A runtime
with neither is refused by name before anything is submitted. Before this,
every path-addressed reset run from a checkout exited 2 with the interpreter's
"can't open file" and left its record failed.

The default only reports, and it sends no deadline unless you pass
`--timeout-s`. It submits at `--priority -10` by default, behind everything
interactive. Duplicates are collapsed by working directory and argv. An
action that had the whole device to itself keeps `--exclusive`, restored from
the GRES the lane recorded, because `{"gpu": 1}` alone cannot say it. A record filed by the SLURM lane always goes back out on the lane whatever
`--transport` says, because re-submitting a lane-filed failure into a queue no
worker drains would lose it. A record that names no transport follows the same
default every producer follows: `PRISMABUILD_TRANSPORT`, then the published
generation's `default_transport`, then the pull queue. Withdrawn actions are skipped: re-submitting them
would undo a decision. A record `pool_reset` has already handled is filed as
`reset` and skipped, unless you pass `--include-reset`. That record keeps the
failure it recorded: the returncode, the output tails, and the job the lane
submitted stay in `detail`, and the reset is stamped beside them under `reset`.

`--apply` keeps each re-submission's output under `<queue root>/resets/` and
waits a few seconds, once for the whole batch, before it stamps anything. A
child `pbrun` that refuses does so at once, and a refusal is printed with the
tail of what it said, leaves its record `failed` so the next run plans it
again, and makes the command exit non-zero. A child still running at the end
of that wait has been admitted and is left alone.

A record addressed by a snapshot, which is what the lane files for every
submission, is not re-sealed. Its tree is a commit in the CAS and nothing can
have moved under it, so `pool_reset` sends the same action back through the
lane unchanged, with the demand, tags, and exclusivity its own ending recorded,
and detaches. It prints the key and the job id, and `pbwait` files the ending.
Such a record goes out on the lane only: under `--transport pool` it is skipped
with the reason. A submission the controller refuses, for an unknown Feature or
an impossible GRES, is reported for that record, the record stays `failed`, and
`pool_reset` exits 1 after handling the rest.

### What each terminal status means

| Status | Meaning |
|---|---|
| `executed` | The work ran. Filed under `done/`. The two transports decide it differently: the lane files `executed` only when the receipt is in the CAS, and the pull queue derives it from the launcher exiting 0. |
| `cache_hit` | The receipt was already there. Counts as done. On the lane, `pbrun` finds it before submitting and submits nothing. A job that starts and finds it -- the second job of a key, held behind the first -- reports it too, before materializing anything. Either way `done/` keeps the record of the run that did the work: a `cache_hit` record is filed only when the key has none. |
| `failed` | No receipt. Something refused, or the command exited non-zero. Filed under `failed/`. |
| `timeout` | The action was killed at a deadline. `returncode` is null, because an action that finished inside the tick that crossed the deadline would otherwise report 0 for a record filed as a timeout: read `status`, not `returncode`. Filed under `failed/`. Retriable. Under SLURM the deadline is the `--timeout-s` you asked for and the scheduler enforces it. In the pull queue `pbrun --timeout-s` is parsed and not sent, so the deadline is the worker loop's own `--timeout-s` on the box that claimed the action. |
| `withdrawn` | Somebody cancelled the run. Not a defect, and not retried. |
| `reset` | A `failed` ending that `pool_reset --apply` re-submitted. The record stays under `failed/` with its `detail` intact and a `reset` object beside it, carrying the reason, the time and the host that reset it. The attempt links move to `attempt_history_before_reset`, so a reader does not adopt the old attempt's `failed` as this record's own ending. See "Reset a batch of failures". |
| `finish_lost_race` | The worker finished work whose claim a reaper had already concluded, and the item's own record was gone, so nothing could be carried forward. Filed under `failed/` with the reason in `detail` and the launcher's own result under `detail.worker_detail`. Only a failing outcome reaches this: a successful one files its real status. |
| `unreadable` | Not a filed status. `pbstatus` prints this row for a record it could not read, so a truncated or unreadable newest record does not print the empty table a fleet that filed nothing prints. The note column names the reason and the path: `permission denied`, the OS error's own text, `not valid JSON`, or `not a JSON object`. No other column carries a value, because every other column is inside the file nobody could read. The row is placed by the file's modification time. |

## Read a failure

Start with `pbstatus`, which names the status, the transport, the host, the
return code, whether a receipt was published, and the SLURM state.

Then read the logs. The terminal record carries the last 256 KiB of each stream
inline under `detail.stdout` and `detail.stderr`; it holds a tail because a
build log on this fleet reaches hundreds of megabytes. The full files stay in
the action's lane directory, named by `detail.slurm.stdout_path` and
`stderr_path`.

### Common refusals

These are refusals at submission, before anything reaches the fleet.

*   **`a non-Git checkout cannot be materialized`** — `--cwd` must be inside a
    Git checkout, so its exact bytes can be sealed and materialized through the
    CAS. Mutable path-addressed submission is not supported.
*   **`--cwd is not a directory on <host>`** — the path may be correct and
    belong to another box. `pbrun` stamps the code closure inside the checkout,
    so it can submit only for a checkout on the box it runs on. Submit from that
    box; the queue is shared, the filesystem is not.
*   **`cannot write into the checkout <dir>`** — the checkout is read-only, or
    owned by someone else, or the disk is full. `pbrun` keeps the closure stamp
    there and the action tees its output into the same tree, so submit from a
    writable clone or worktree of it.
*   **`command executable is absent or not executable on the submitting box`** —
    `pbrun` resolves `argv[0]` exactly against the declared `PATH`. Pass `--tag`
    for the worker class that owns the executable, or `--anywhere` to assert an
    identical executable contract on every eligible worker.
*   **`direct argv or caller environment names an external path absent from the
    submitting box`** — the same rule applied to a path-shaped argument. This is
    a conservative lexical screen, not a parser: a dependency named inside a
    config file or a shell string is still yours to declare.
*   **`executable script bytes are outside the snapshotted repository`** — move
    each helper under the repository so its bytes are bound by the action's code
    closure.
*   **`checkout snapshot symlink points outside the sealed repository`** — a
    symlink in your checkout reads bytes the snapshot does not carry. `pbrun`
    resolves the whole link graph, so the escape can be composed out of links
    that each look contained: with `a -> .` in the tree, `b -> a/../outside.txt`
    reaches the repository's parent. Point the link inside the repository, or
    declare the external bytes as an input. A worker applies the same rule to
    the tree it checks out and refuses with `materialized checkout symlink
    points outside the sealed repository`, which is what an older snapshot
    already in the queue reports. A link the worker's filesystem cannot follow
    at all, which for an older snapshot means a loop among its links, refuses
    with `materialized checkout symlink cannot be resolved`.
*   **`slurm refused this action`** — `sbatch` rejected the submission. The
    message names the required tags and the demand. An unknown Feature is the
    usual cause: a tag that no node carries can never be scheduled. Read
    `sinfo -N -l` for a node that offers it, or fix the `--tag`.
*   **`the fate of this submission is unknown`** — `sbatch` stopped answering
    and the controller could not say whether it took the job. This is not a
    refusal and it is exit 75, not 1. Run the `squeue` in the message. A job
    listed with that `Comment` is yours and is running with no submission
    record; withdraw it with `scancel` and submit again. No such job means
    nothing was submitted.

Two failures happen after the job ran.

*   **`slurm job <id> exited 0 but published no receipt`** — the job ended
    cleanly and did no work. Read the job's `.out` and `.err`; the worker's own
    refusal is there.
*   **`no scheduler command can describe slurm job <id> ... and it has published
    no receipt`** — the controller knows no such job. It may still be running,
    or it may have been purged past `MinJobAge`. This is exit 75, not a failure:
    there is no verdict yet. Look under the action's lane directory.

## Example consumer: Tessera

PrismaBuild is not coupled to any project. Tessera is one consumer, and its
dispatch tools show the shape a producer takes when `pbrun` is the wrong entry
point.

`tools/fleet/dispatch_tessera_shards.py` and
`tools/fleet/dispatch_tessera_ladder.py` seal their own action bodies — their
own closures, environments and result paths — one action per input shard.
Routing them through `pbrun` would re-seal the work as a shell command and lose
exactly that.

What they share with `pbrun` is the last step: hand the sealed action to
whichever transport is live. That step is `tools/fleet/fleet_submit.py`, and
it is there once.

The SLURM lane addresses a checkout only through the action's sealed snapshot,
because a `checkout_root` is a path the submitter chose and the scheduler may
place the job where that path means something else. So a checkout root given
for the lane is sealed at submit time, through `pbrun`'s own snapshot builder,
and the action that reaches the scheduler is the snapshot-addressed one the
node needs. Sealing moves the action key once, and only for an action that
carried no snapshot; a producer should print `Submission.action_key`.

A producer addresses its own code relative to the tree the action runs in.
`fleet_submit` runs `pbrun`'s relocation guard over the action's argv and
environment while it seals, so an absolute path into the submitter's checkout
is refused before anything is queued. A sealed snapshot the executing process
never imports is not provenance: the worker verifies the sealed bytes and the
interpreter loads the shared ones. Both Tessera dispatchers therefore set
`PYTHONPATH` to `tessera/src`. That is a different action key from the absolute
spelling they used before 2026-09-05, so receipts published under the old keys
are misses and those shards re-encode.

`fleet_submit` files no endings. It returns as soon as the scheduler has the
job, and the lane's submission record is what makes the job findable
afterwards. Run `pbwait` on the keys to derive and file the terminal records.

`tools/fleet/tessera_status.py` reads the export's progress from the CAS
receipts of the export it names, not from files in the shared checkout. Under
SLURM a shard writes its manifest inside a private checkout the job removes
when it ends, so the receipt is the record. The export is identified by the
digest of the allocation plan the dispatcher hands the exporter, so a receipt
from a previous plan is counted on its own line instead of deciding the shard
count. The screen reads the shared results directory only under the pull
queue, which is the transport that wrote those files. It reports what it could
not read rather than failing.

Any producer that builds its own actions should do the same: seal the action,
hand it to `fleet_submit`, print the key, and read the CAS for the verdict.

## Publish the runtime the fleet executes

A worker does not run your checkout. It runs
`/mnt/shared/prismabuild-fleet/repo`, the copy on the one filesystem every box
mounts. A fix you commit here changes nothing on the fleet until that copy is
republished.

**Publishing a generation, and cutting the fleet over to a transport, need
Rob's explicit word and an idle queue.** That is a standing constraint of the
campaign freeze, not a suggestion, and it holds even when the change looks
small. The install and the cutover are his to run
(`fleet/slurm/install.sh`, then `fleet/slurm/cutover.sh`); see the
[README](../README.md) and the [SLURM install
runbook](slurm_runbook_2026-09-04.md).

`tools/fleet/publish_runtime.py` is the mechanism. It does not copy over the
live bytes. It builds a complete new generation under
`/mnt/shared/prismabuild-fleet/runtime-generations/`, named for the commit, the
time and a nonce, then moves `repo` onto it in one namespace operation. A
reader therefore sees one whole generation or the previous one, never a
half-copied mixture.

    tools/fleet/publish_runtime.py --dry-run
    tools/fleet/publish_runtime.py

`--dry-run` prints the commit and every file that would be published, and
writes nothing.

Each generation carries `RUNTIME_VERSION.json`: the commit, whether the tree
was dirty, the generation name, who published it, and a sha256 for every
published file. The receipt is what makes a disagreement between a box and this
checkout a fact rather than a suspicion. A worker's own attestation records the
resolved path, the size and the sha256 of the core module it loaded and of the
launcher script that started it, and it refuses if either changed after it was
captured, so the two sides can be compared after the fact.

Publication refuses rather than guesses:

*   **A dirty tree** is refused unless you pass `--allow-dirty`, because the
    receipt would name a commit whose bytes are not the bytes published.
*   **A checkout that moves while the copy is staged** is refused. The commit,
    the dirty flag and every file digest are re-proved after the copy and
    before the receipt is written.
*   **A generation that fails its import probe** is refused. The staged tree is
    imported off to one side before anything is activated.
*   **A generation store this user cannot write** is refused before the tool
    says it is publishing anything. The live runtime is untouched either way.
*   **A live `repo` that is still the legacy plain directory** is refused
    without `--migrate-directory`. Replacing a directory with a symlink is
    not one atomic operation on this NFS mount, so that one-time handoff
    retains the old directory beside the generation store and rolls the name
    back if the install fails. A caller can be refused in that narrow
    interval. It can never read a mixed generation.
*   **A live `repo` that is neither a directory nor a symlink** is refused.

A published generation is sealed read-only and is never deleted. That is what
makes rollback a namespace operation:

    tools/fleet/publish_runtime.py --activate-generation <name>

Rollback is deliberately not a re-publication. The old generation's bytes and
receipt were proved when it was published, and rebuilding them from a checkout
that has moved would not be the same thing. A name that is not a direct child
of the generation store, a dot-name, or a directory with no receipt is refused
before `repo` is touched. A dot-name matters: a staging tree left by an
interrupted publish carries a receipt but was never sealed or probed.

### The default transport rides in the generation

`--default-transport pool|slurm` records `default_transport` in the receipt.
`fleet_submit.default_transport` reads `PRISMABUILD_TRANSPORT` first, then that
field, then falls back to the pull queue. The field is optional, and absent
means the pull queue, so every generation published before the SLURM cutover
keeps the behaviour it had.

The default rides in the bytes because a fleet has no single environment to
export into. Agents start `pbrun` from a crontab, from user units, and from
each other on three boxes. Pointing `repo` back at the previous generation
restores the previous default in the same atomic operation that changed it.

### Who reads the generation

*   `pbrun` reports the published commit, so a submission can say which bytes
    the fleet is serving.
*   A worker loop holds the module it imported for its whole life. It compares
    the commit beside those bytes against the commit at the live `repo` name on
    each idle poll, which is how it notices a successor was published.
*   `supervise` treats the live generation and every published generation
    behind it as legitimate, because an old generation may still be running an
    action. A tree that is neither is not this fleet's, whatever the script
    inside it is called.

## Smoke-test a transport

`tools/fleet/seal_and_publish.py --transport pool|slurm` seals one trivial
action and hands it to the named transport. It prints the submitted key, the
key it sealed, and where the submission went, so a `ready/` item under the
pull queue and a submission record under the lane are told apart.

The SLURM lane addresses a checkout only through a sealed snapshot, so this
command makes its smoke checkout sealable: if
`/mnt/shared/prismabuild-fleet/checkout` has no commit, the first run
initializes a Git repository there and commits `task_code.py`, and nothing
else. A checkout that already has a commit is left as it is. Before
2026-09-05 the command wrote a plain directory, and `--transport slurm`
refused every run with `a non-Git checkout cannot be materialized` without
reaching a scheduler command.

A refused submission now prints the transport's reason on stderr and exits 2.

Initializing the checkout is necessary but not sufficient, and this part
applies to both transports. `run_local_action` refuses an action whose
declared result already exists in the execution tree with no recovery claim,
and a claim is keyed by action key, so a result left by an older key is not a
claim for the current one. `/mnt/shared/prismabuild-fleet/checkout` currently
holds `fleet_result.txt`, `pbrun_result.*`, `.pbrun-closure.*` and 120 files
under `results/glm53-tessera/`. The pull queue executes in that tree itself,
so a re-dispatch under a moved key refuses on every shard that has a file
there rather than re-encoding. The SLURM lane reaches the same files by a
different route: the snapshot roster is tracked plus nonignored-untracked
paths and the checkout has no `.gitignore`, so those files ride into every
snapshot and the node refuses there too.

Move them out of the checkout before the next dispatch from that tree on
either transport. Ignoring them is the SLURM half only, because the pull queue
never snapshots. This applies to the smoke action, whose result is
`fleet_result.txt`, and to every export shard, whose result is
`results/glm53-tessera/shard-NNNNN.json`.
