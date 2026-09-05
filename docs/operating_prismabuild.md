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
runtime generation carries a default that applies when neither is set.

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
    Edits you make after submitting cannot change what runs.
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

Submit a command with `pbrun`. Everything after `--` is the command.

    tools/fleet/pbrun.py --gpu --timeout-s 3600 -- ./stage.sh --shard 3

`pbrun` waits for the action and exits with the result. `--cwd` selects the
checkout to seal; it defaults to the current directory and must be inside a Git
checkout on the box you submit from.

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

Every job is submitted with `--no-requeue` and `--export=NIL`. Only SLURM's own
variables reach the job; the action's environment is the sealed one the worker
builds. `--chdir`, `--output` and `--error` point at the action's own lane
directory. Retries are new submissions with new job ids, never `--requeue`. The
[install runbook](slurm_runbook_2026-09-04.md) shows a full `sbatch` line.

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

### Exit codes

`pbrun` and `pbwait` use the same codes.

| Code | Meaning |
|---|---|
| 0 | The work is done. A `cache_hit` counts as done. |
| 1 | The action failed. `pbrun` prints the worker's message and the log paths. |
| 2 | `pbrun --withdraw` matched no submission, matched more than one, or `scancel` refused the job. Also argparse's own usage error. |
| 75 | The wait ended before the work did. Nothing was cancelled. |
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

After a 75, run `pbwait` on the key. The job is still queued or running, and
under SLURM `pbwait` is what files the ending once it stops.

### No deadline exists by default

`pbrun` sends no `--time` unless you pass `--timeout-s`. A job that is doing
something runs until it ends. Elapsed time is never treated as evidence that a
worker is dead.

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
those records are files on the shared mount. `--json` prints one object with the
three lists and any scheduler notes.

The underlying commands are `sinfo` for nodes, `squeue` for jobs, and `sacct`
for jobs the controller has forgotten. Use them directly for scheduler detail
`pbstatus` does not join in.

### Where the records live

| Location | What is there |
|---|---|
| `/mnt/shared/prismabuild-fleet/pb-queue/done/<key>.json` | The ending of an action whose work was done. |
| `.../pb-queue/failed/<key>.json` | The ending of an action with no receipt. |
| `.../pb-queue/withdrawn/<key>.json` | The marker for an action somebody cancelled. |
| `.../slurm/<key>/` | The lane directory: `job.sh`, `submissions/`, `latest.json`, `liveness.jsonl`, and `<jobid>.out` and `.err`. |
| `.../cas/` | The content-addressed store: action requests, results, and receipts. |

Both transports file their endings in the same two directories, so a SLURM
ending and a pull-queue ending appear side by side. A record's `transport` field
says which filed it.

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

A withdrawal cancels the run, not the name. The marker is scoped to the
generation it was filed against, and a later submission of the same key retires
it into `withdrawn/superseded/`: the action key is a content hash, so
re-submitting it is how anybody asks for the same work again. If the action
finished a moment before you asked, `pbrun` says an outcome is already filed and
withdraws nothing.

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

The default only reports, and it sends no deadline unless you pass
`--timeout-s`. It submits at `--priority -10` by default, behind everything
interactive. Duplicates are collapsed by working directory and argv. An
action that had the whole device to itself keeps `--exclusive`, restored from
the GRES the lane recorded, because `{"gpu": 1}` alone cannot say it. A record filed by the SLURM lane always goes back out on the lane whatever
`--transport` says, because re-submitting a lane-filed failure into a queue no
worker drains would lose it. Withdrawn actions are skipped: re-submitting them
would undo a decision. A record `pool_reset` has already handled is filed as
`reset` and skipped, unless you pass `--include-reset`.

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
| `executed` | The work ran and published a receipt. Filed under `done/`. |
| `cache_hit` | The receipt was already there. Counts as done. On the lane, `pbrun` finds it before submitting and submits nothing; `done/` keeps the record of the run that did the work, and a `cache_hit` record is filed only when the key had none. A receipt that lands between that check and the job's start is found by the node instead, and the job files `executed`. |
| `failed` | No receipt. Something refused, or the command exited non-zero. Filed under `failed/`. |
| `timeout` | SLURM killed the job at a `--timeout-s` you asked for. `returncode` is null. Retriable. |
| `withdrawn` | Somebody cancelled the run. Not a defect, and not retried. |

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
*   **`slurm refused this action`** — `sbatch` rejected the submission. The
    message names the required tags and the demand. An unknown Feature is the
    usual cause: a tag that no node carries can never be scheduled. Read
    `sinfo -N -l` for a node that offers it, or fix the `--tag`.

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

`fleet_submit` files no endings. It returns as soon as the scheduler has the
job, and the lane's submission record is what makes the job findable
afterwards. Run `pbwait` on the keys to derive and file the terminal records.

Any producer that builds its own actions should do the same: seal the action,
hand it to `fleet_submit`, print the key, and read the CAS for the verdict.
