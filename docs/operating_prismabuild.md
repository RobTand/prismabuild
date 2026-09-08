# Operating PrismaBuild

This guide is for the operator or agent who puts work on the fleet. It covers
submitting a command, waiting for it, running a campaign, watching what the
fleet is doing, stopping work, and reading a failure.

The live fleet uses the pull queue (`pool`), with per-attempt containment and
adaptive CPU and GPU admission. SLURM (`slurm`) is an optional transport; its
[install runbook](slurm_runbook_2026-09-04.md) and historical
[scheduler decision](scheduler_decision_2026-09-04.md) describe that deployment
path. See [the design document](design.md) for the current system contracts.

Both transports publish the same verifiable CAS results. Their placement and
resource enforcement differ, as described below. Examples name
`--transport slurm` where SLURM behaviour is the point. You can set
`PRISMABUILD_TRANSPORT=slurm` instead, and the published
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
checkout on the box you submit from. `pbrun` injects its closure stamp into a
private Git index while sealing the snapshot; it creates no stamp or scratch
file in the submitting tree. The worker verifies that stamp and writes its
result in the materialized checkout. A read-only source checkout works when
its Git excludes are already configured; first-time exclude setup still needs
write access to Git's common `info/exclude` file.

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
| `--gpu` | Defaults to `gpu=1,mem_gb=16`; explicit demand overrides the defaults. Pool generation actions permit adaptive sharing. | `--gres=shard:1`, partition `gpu`. |
| `--demand gpu=1,cpu=8,mem_gb=32` | Aggregate resource demand. Without `--gpu`, `mem_gb` defaults to 4; `cpu` defaults to `--cpus`. | `--gres=shard:1 --cpus-per-task=8 --mem=32768M`. |
| `--gpu-memory-gb N` | GPU memory budget in GiB; requires GPU demand. Pool only. See the memory-domain rules below. | Refused: this lane does not enforce a separate VRAM budget. |
| `--exclusive` | Reserve one box's whole GPU capacity; implies GPU demand and at least 16 GiB host memory. | `--gres=gpu:1` rather than a larger shard count. |
| `--gpu-capacity N` | Explicit capacity override for `--exclusive`; normally leave it unset so worker offers supply the physical capacity. It does not set shared job concurrency. | Under SLURM, only `1` is accepted: `--gres=gpu:1` is the whole device, so a larger count would be read and discarded. |
| `--cpus N` | Cores the action will actually use. Defaults to 1. | `--cpus-per-task=N`. |
| `--tag NAME` | Require a box offering this tag. Repeatable. | `--constraint=NAME`, ANDed with `&`. |
| `--here` | Pin the action to this box. Combines with `--tag`. | The box's hostname joins the constraint. Every hostname is a node Feature. |
| `--anywhere` | Assert that dependencies outside the snapshot are identical on every eligible worker. | No constraint, and the default partition. |
| `--priority N` | A queue hint. Higher runs sooner; a negative value yields to everything at 0, and aging never lifts it past them. Defaults to 0. | `--nice`, sent on every submission. SLURM subtracts the nice from the base priority its scheduler assigned. |

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

Ready items are ordered by priority band first, then by admission denials
(aging), then by publish time. Aging reorders only within a band: a
`--priority -10` item denied a hundred times still sorts after a fresh
priority-0 item, and because a claim walks that order and a withhold ends the
pass, nothing at a negative priority is considered until every item above it
has been tried. That is what makes `-10` mean "only when nothing else wants
the box". Agent self-validation -- test shards, the receipt for a PR, a re-run
to confirm a fix -- submits there (`pbtest.py --priority -10`, `pbrun.py
--priority -10`) and cannot displace campaign work in the queue.

Restartable background work can also yield the box. When a foreground item
(priority >= 0) is denied admission and one background holder on that box is running whose tokens,
released, would let the denied item in, the worker withdraws that holder through
the ordinary withdrawal ladder and re-publishes it at its own priority. The
background action is retried later; partial output follows ordinary withdrawal
cleanup. Each interruption consumes one of its `max_attempts`, and its aging
count survives. Eligibility requires explicit `--retry-safe`, a remaining
attempt after the interruption, and a verified generation action. Measurements,
unknown action identities, and holders with recorded failures are left running.
Keeping the latter in their original generation preserves their complete failure
history. Negative priority alone does not authorize a restart.

The existing attempt counter bounds both interruptions and later failures.
Earlier interrupted generations are linked through immutable withdrawal
decisions using `supersedes_withdrawal.published_unix`, while
`attempt_history_missing_before` accounts for that prefix in the new generation.
A three-attempt action interrupted twice has one launch left; if it fails,
it ends failed rather than receiving another three attempts.

The release is not immediate. `withdraw` returns `released 0`: the holder's own
worker stops the payload and returns the tokens at its next checkpoint, so the
foreground item is admitted on a later poll rather than on the pass that
preempted. Four bounds keep the cost honest: only a foreground denial triggers
it, only an eligible `priority < 0` holder is selected, only a holder whose release
actually closes the gap is stopped, and only one at a time -- tokens a
withdrawing holder is about to return are counted as promised, so a second pass
does not cancel a second action for the same gap. A holder that cannot be
stopped through the ladder -- already withdrawing, waiting on cleanup, or on
another box -- is skipped, never forced.

`pbstatus` shows the cost. The requeued row in the jobs table names the
preemption in its NOTE, and the withdrawal in the endings table reads
`preempted by <key12>`; both carry `preempted_by` as a field in `--json`. The
immutable withdrawal decision under
`withdrawn/decisions/<key>/<generation>.json` carries it too.

A submitter waiting on a preempted action is not told it was withdrawn. The
requeue is a new generation, and a waiter names the generation it submitted, so
`pbrun` follows a cancellation stamped `preempted_by` to the generation the
requeue published and reports *that* run's ending -- the retry's, whether it is
still queued, running, or already done. An operator's withdrawal is unaffected:
it carries no `preempted_by`, so it ends the wait as it always did, and so does
a preemption whose requeue was never published, because then the cancellation is
the whole account of what happened.

### SLURM partitions

The optional SLURM lane derives the partition from demand and placement, so it
adds nothing to the action's identity:

*   A GPU demand goes to the `gpu` partition, the only place shards exist.
*   `--anywhere` on CPU-only work goes to the default partition `all`, where
    node weight prefers dl380g10 and a GB10 box takes it only when dl380g10 is
    full.
*   CPU-only work with no tag goes to the `cpu` partition.
*   Tagged work goes to the default partition, where the sealed `--constraint`
    picks the node.

### What a SLURM submission sends

Every SLURM job is submitted with `--no-requeue`, `--export=NIL` and
`--dependency=singleton`, under the job name `pb-<first 12 characters of the
key>` and with `--comment=pb:<key>:<attempt>:<nonce>`. Only SLURM's own variables reach the job; the action's environment is
the sealed one the worker builds. `--chdir`, `--output` and `--error` point at
the action's own lane directory. Retries are new submissions with new job ids,
never `--requeue`. The [install runbook](slurm_runbook_2026-09-04.md) shows a
full `sbatch` line.

### SLURM singleton submissions

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

### Demand and containment

Live pool workers launch each attempt inside its own broker-owned cgroup. The
scope includes direct payloads, descendants and owned Docker containers, with
an admitted CPU mask and a hard host-memory limit. The broker stops an attempt
that exhausts its aggregate limit; its reservation stays held until exact-scope
cleanup completes. GPU allocations also need broker accounting: on GB10, CUDA
memory can escape ordinary cgroup charging, so the GPU memory guard enforces the
shared physical budget. See "Adaptive GPU admission and memory budgets" below.

Under optional SLURM, CPU and host-memory demand become `--cpus-per-task` and
`--mem`, while `cgroup.conf` constrains cores, memory and devices. Its configured
GPU shards are a SLURM scheduling resource, independent of the live pool's
adaptive sharing. This lane does not implement `--gpu-memory-gb` enforcement.

Declare the aggregate peak CPU and memory used by the entire action. A test run
with eight pytest processes should reserve their combined resources and bound
native threads per process; admission cannot infer those needs from argv.

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
| 74 | A filesystem or record-persistence error prevented `pbrun` or `pbwait` from completing the operation. The diagnostic distinguishes a known accepted job from an unverified submission, and says whether a withdrawal reached `scancel`. |
| 75 | No verdict yet. The wait ended before the work did, or `sbatch` stopped answering and the controller could not say whether it took the job. Nothing was cancelled and nothing was filed. |
| 143 | The action was withdrawn. 128 + SIGTERM, the signal a withdrawal sends. |

Those four codes are `pbrun`'s own, and a run never gets to speak them. A run
whose recorded status is 2, 74, 75 or 143 is reported as 1, with its real
number on a line of its own and under `detail.returncode` in the record. Every
other status reaches the caller unchanged: a run that exits 7 exits 7. The case
this exists for is 143. `core._sigterm_unwinds_this_process` raises
`SystemExit(128 + signum)`, so any SIGTERM that is not a withdrawal leaves 143
in the record, and a caller reading that was told an operator had made a
decision that nothing on disk records.

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
      record:    /mnt/shared/prismabuild-fleet/pb-queue/done/.<key>.json.<pid>.<uuid>.tmp
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

`pbwait` uses the same diagnostic when it cannot file a detached job's ending,
returns a `record_error` row naming the job and failed path, and exits 74.
A CAS read failure or a filesystem failure before submission instead says
that filesystem access failed; it does not claim a record write was attempted
or that SLURM accepted a job. The job id is retained when already known.
If the CAS itself cannot be read, receipt status is reported as unknown.

Withdrawal has two write stages. If its initial marker or terminal record
cannot be written, no cancellation is sent. If `scancel` accepted the
cancellation but the acceptance stamp cannot be written, the diagnostic says
so. Both exit 74 and give the withdrawal command to retry after restoring
record writes. The path in these diagnostics may name a temporary file;
atomic record publication creates that file before renaming it.

An unreadable terminal record that appears while a pull-queue wait is polling
ends the wait immediately with exit 1 and the path and reason, instead of
spending the remaining deadline and reporting exit 75. For a recorded SLURM
submission, `pbwait` first tries to recover the ending from the controller and
CAS; if it cannot repair the record, it reports an `unreadable` row.

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

### Execution deadlines

`pbrun --timeout-s` seals a positive finite execution budget into the action.
Changing that budget changes the action key. SLURM receives the corresponding
`--time`; the pool applies the shorter of that budget and its worker's
`--timeout-s` safety ceiling (7200 seconds by default). Without an explicit
budget, SLURM receives no `--time` and the pool retains its worker ceiling.
Pool execution timing starts after checkout materialization, immediately before
launching the worker, using a monotonic clock. Queue waiting does not consume
that budget; `--wait-s` controls the submitter's wait separately. A short budget
does not wait for the next lease heartbeat before being enforced.

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
| `gpu_memory_gb` | `--gpu-memory-gb`, a positive finite GiB budget; pool only, requires GPU demand |
| `priority` | `--priority` |
| `measurement` | `--measurement` |
| `host_class` | `--host-class`, a pool measurement worker class or SLURM Feature such as `gb10` |
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

These rows are refused at load as well, each for the reason `pbrun` gives at
submit:

*   `gpu_memory_gb` without positive `demand.gpu` or `exclusive`, or under
    `--transport slurm`. The budget accepts a JSON number or numeric string
    converting to between 1 and `2**63 - 1` bytes, just like `pbrun`.
    Booleans, non-finite values and out-of-range budgets refuse before any row
    is submitted. For example, `"demand": {"gpu": 1, "mem_gb": 80},
    "gpu_memory_gb": 32` retains the 80 GiB aggregate budget and 32 GiB GPU cap.
*   `measurement` without `host_class` under `--transport slurm`. A SLURM
    measurement is keyed on the scheduler-attested class that produced it.
    Under `--transport pool`, omitting `host_class` is the default form: the
    submitter's platform/toolchain is sealed and its hostname is added to
    placement implicitly.
*   `measurement` with `anywhere` under `--transport pool`. A measurement must
    retain its submitting-host or explicit class placement and attested facts.
*   `host_class` without `measurement` under `--transport pool`. The pool's
    explicit class-placement option applies only to measurements; SLURM also
    supports controller-attested class-keyed generation.
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

For the pool, omit `host_class` to keep every row implicitly pinned to the host
running `pbcampaign`, and its platform/toolchain becomes part of the action:

    [
      {
        "argv": ["./probe.sh", "--shard", "0"],
        "cwd": "/home/rob/mypkg",
        "measurement": true,
        "retry_safe": true
      }
    ]

Run that form with `--transport pool`. Do not set `anywhere` on those rows.

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

`--workers-per-shard N` runs N pytest workers in each action with `pytest -n N`.
The default is 1 and needs no plugin; higher values require `pytest-xdist` in
the target interpreter. The coordinator still uses only the standard library.
For example, `--shards 16 --workers-per-shard 5 --threads-per-shard 1` can
use 80 CPU cores across 16 concurrent actions, subject to available fleet
capacity and sufficient memory per shard. `--mem-gb` reserves memory for the
whole action, including all of its pytest workers.

`--threads-per-shard` sets each pytest worker's BLAS and OMP ceiling. The
CPU reservation defaults to workers times threads; `pbrun --cpus` carries it
into the lane's `--cpus-per-task`. `--cpus-per-shard N` can reserve more, but
cannot reserve fewer than this product. With `--threads-per-shard 0`, no
native thread ceiling is set, so an explicit CPU reservation is required and
must allow at least one core per pytest worker. Invalid worker counts,
negative thread ceilings, and insufficient reservations are refused with
exit 2 before any action is submitted.

The CPU demand is sealed into each shard's action, so a suite fanned out at a
different width is a different action rather than a cache hit of the last run.

Each shard prints its pytest terminal summary, and `--json` records the same
text plus a `ran` flag. `ran` is true when pytest reported a terminal summary
— the `1 failed, 531 passed, 1 skipped in 17.82s` line — and false otherwise.
A shard that died before or outside pytest prints `NO PYTEST SUMMARY`, the
number of files that did not run, and how the shard ended (`rc=N`, or
`signal N` when it was killed), because a shard starved of I/O, one whose
submission was refused, and one that ran clean are three different events that
used to print the same blank.

### Choose the project's test environment

`pb-cpu` is the PrismaBuild infrastructure test environment. Its presence on a
worker does not mean that arbitrary project dependencies are installed there.
For PrismaQuant CPU tests on the current x86 fleet, use the existing project
interpreter `/home/rob/venvs/pq-cpu312/bin/python`:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py \
  --checkout /path/to/prismaquant \
  --python /home/rob/venvs/pq-cpu312/bin/python --tag x86 \
  --workers-per-shard 2 --threads-per-shard 1 --mem-gb 6 \
  tests/test_shipcard_git_provenance.py tests/test_format_registry.py
```

The same interpreter and `--tag x86` work with `pbrun`. This class constraint
declares the external environment dependency and allows every eligible x86
worker; it does not consume a GB10 just to obtain Python packages. Provision
and qualify this environment before adding another worker to that population.
Do not replace the tag with `--anywhere`: this interpreter is not installed on
the current GB10 workers, and `--anywhere` asserts that external dependencies
are available throughout the eligible population. PB does not infer a project's
Python imports from the checkout or install its packages at submission.

The project environment has CPU PyTorch and the common PrismaQuant runtime and
test dependencies, including `compressed_tensors`, which its autouse fixture
imports even for otherwise dependency-light tests. Tests requiring Tessera's
producer source must additionally declare their pinned Tessera dependency;
tests requiring CUDA, model data or a serving runtime retain those requirements.
The [CPU environment qualification](prismaquant_cpu_environment_2026-09-07.md)
records versions, exact tested scope, receipts and the missing-dependency
reproduction. It does not certify the entire PrismaQuant suite as CPU-portable.

## Submit a measurement

A measurement's numerics do not transfer across architectures, so every
measurement has a nonportable execution scope. The two transports establish it
differently.

For the live pull queue, submit from the box whose platform should produce the
result:

    tools/fleet/pbrun.py --transport pool --measurement -- ./probe.sh

This seals `execution_scope.portability=platform_keyed`, with the platform key,
executable digest, ABI and accelerator facts derived from the submitting box's
live evidence. `pbrun` also adds that box's hostname to effective placement
without requiring `--here`. The worker re-derives and verifies the platform and
toolchain before execution. `--anywhere` is refused because a measurement must
retain its declared nonportable scope.

When the complete experiment and its external dependencies are identical across
a worker class, explicitly let PB select a matching worker:

    tools/fleet/pbrun.py --transport pool --measurement --host-class gb10 --gpu -- ./paired-probe.sh

The class enters sealed placement and the scope remains `platform_keyed`.
The worker must match the submitter's platform, libc ABI, shell executable,
driver, and GPU models/counts and compute capabilities before running. The
receipt records the selected worker and actual GPU UUID. Unknown device identity
refuses. `--here` adds a host pin even with a class; `--anywhere` is unnecessary
and refused. The class is placement intent, not a claimed SLURM attestation.

Keep both arms of a comparison in one self-contained interleaved action. CPU
near-idle admission, measurement isolation, GPU exclusivity, memory and telemetry
gates remain enforced. This option changes eligibility, not available capacity.
Pin and record container images and inner Python dependencies in the experiment:
the worker attests pbrun's shell and host facts, not arbitrary inner environments.
Declaring the class asserts that these external dependencies are identical on
its workers. Shared mutable container tags are not immutable toolchain evidence.

The cache key binds the class and attested compatibility facts. Changing the
architecture, GPU model or declared toolchain produces a different key; changing
only the selected matching host or physical GPU does not. A hit replays the
recorded experiment and its producer identity, not a new timing of this host.
Do not resubmit already running experiments merely to move them between hosts.

For SLURM, retain the explicit host-class form:

    tools/fleet/pbrun.py --transport slurm --measurement --host-class gb10 -- ./probe.sh

`--host-class CLASS` names a node Feature. It seals `execution_scope
host_class_keyed`, joins the effective placement so the action key moves with
it, and the SLURM lane sends it as `--constraint=CLASS`.

The SLURM constraints are enforced rather than advised:

*   **A SLURM `--measurement` refuses without `--host-class`.** Its class must
    be present in the sealed scope and scheduler constraint.
*   **SLURM's `host_class_keyed` scope requires controller evidence.** The pool
    class-placement option above uses actual platform facts instead.
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

When ready work persists while a host claims nothing, inspect that host's
worker logs as well as `pbstatus`. `host admission lock busy; observed holder
pid=...; candidate evaluation not reached` identifies refusal at the local
admission FLOCK before per-action placement and resource checks. Each loop
reports at most once per 60 seconds; `unknown` means the holder could not be
read. The PID is an observation, so establish its current identity and lock
ownership before any recovery action. This message neither diagnoses the
holder's filesystem wait nor implies that the ready queue is empty.

`pbstatus` answers the three questions in one command, with no arguments:

    tools/fleet/pbstatus.py

It follows `--transport pool|slurm`, then `PRISMABUILD_TRANSPORT`, then the
deployed runtime's default, just like submission. With pool transport it reads
the shared queue and needs no SLURM binaries. `--transport slurm` explicitly
selects the controller view.

It prints three tables:

*   **nodes** — for pool, each worker's declared and observed capacity, offer
    age, and freshness of persisted CPU/GPU admission evidence. Expired offers
    remain visible as stale. For SLURM, the controller's nodes, partitions,
    CPU/memory allocation, GRES, features, load and reachability.
*   **jobs** — for pool, ready and claimed action keys, resources, placement,
    admission denial counts and lease age. Ready jobs use the pool's existing
    placement rules to name matching live workers; matching is capability,
    not a promise of immediate admission. Missing/stale leases explicitly say
    process liveness is unknown. The `RELEASES` column counts the times this
    action was returned to `ready` without starting — a claim whose lease
    never appeared and whose attempt was never published is released uncharged
    rather than concluded — and a ready job's note repeats it, because a key
    the reaper keeps handing back otherwise reads exactly like a key nobody
    has got to yet. A blank column is an action that has never been released.
    For SLURM, queued/running jobs joined against lane submission records,
    including constraints and submitting hosts.
*   **endings** — how the last actions ended, newest first, each labelled with
    the transport that produced it. `--recent N` changes how many are read; the
    default is 20.

`pbstatus` never writes, and an unavailable part of the fleet is reported in
place rather than failing the screen: the tables that were read still print.
Since #358 that report is also in the exit status, because printing a note is
not enough for a wrapper that reads only the lists -- see the exit rule below. Scheduler commands have bounded timeouts, and since #350 so does the
whole run: `--timeout-s` (default 10) bounds every read of the queue root, and
`--timeout-s 0` restores the unbounded behaviour a caller may still want. A
selected SLURM controller that is not
installed prints one line saying so, and the endings table still prints, because
those records are files on the shared mount. A record it cannot read prints as an
`unreadable` row whose note names the path and the reason, so a truncated or
unreadable newest record does not read as a fleet that filed nothing. `--json`
prints one object with the selected `transport`, three lists, any scheduler
notes, and the four fields that say how much of the fleet was actually read:
`complete`, `timed_out_sections`, `unavailable_sections` and
`abandoned_children`. In pool mode its `pool` summary carries ready/claimed
counts and an
`empty` field: `true` means both active directories were read and contain no
jobs, `null` means state could not be established. Corrupt active records stay
visible as `UNREADABLE` rows. The census is not atomic, and persisted admission
samples are historical evidence with explicit freshness, not newly computed
admission decisions.

### When the mount does not answer

A diagnostic that can wait forever is worse than one that says it could not
read the mount, because a hang and a dead fleet look identical from outside.
On 2026-09-07 fifteen `pbstatus` processes sat 33-49 minutes each in
`__nfs_lookup_revalidate` on sparky. They were `hard`-mount waits doing what
`hard` is for, not driver wedges, and every one of them exited on its own when
the stall cleared -- so nothing needed reaping and the answers all arrived,
long after anybody could use them. What they cost while present was measured:
0.0% CPU, 282 MB, and a load average inflated to ~14.7 on a box whose GPU was
idle at 3%, which is a number every human and every agent glancing at the box
then reads.

So the run has a deadline.

*   `--timeout-s N` bounds the pool census and runtime transport lookup with
    one shared budget, not a fresh budget per section: a slow first read
    does not buy the second one a fresh budget. The default is 10 seconds,
    which is two orders of magnitude longer than a healthy census and shorter
    than a person's patience. `--timeout-s 0` waits indefinitely, which is the
    behaviour before #350, and it is the only value that asks for that: `nan`,
    `inf` and `-inf` parse as floats and are refused, because NaN would select
    the unbounded path silently and infinity would reach `select` as the very
    wait the deadline exists to end.
*   Each read of the queue root runs in a forked child the parent abandons at
    the deadline. A `stat` on a hard mount need not return at its caller's
    deadline even after a signal, so no in-process timeout -- thread, alarm or
    otherwise -- can bound it; only a separate process can be left behind. The
    parent never joins a child that may still be blocked. This is the shape
    `tools/fleet/mount_latency.py` already uses for the same reason.
*   That child is handed `/dev/null` on its three standard streams and none of
    the caller's other descriptors before the read starts, because a
    descriptor is not a private copy and this is the child that may outlive
    the run. A reader that kept the caller's table would hold the write end of
    a wrapper's capture pipe, so the wrapper waits for EOF on a command that
    has already exited **3**; and it would hold any `flock` the caller had
    open, since the lock lives on the open file description the fork shares
    rather than on the process. Both were reproduced by the independent review
    of #358 and are covered by `tests/test_pbstatus_review_boundaries.py`.
*   On expiry the tables that were read still print, each missing section is
    replaced by a line naming itself as incomplete, one line goes to stderr --
    `pbstatus: incomplete -- queue root did not answer within 10s (pool,
    endings pending)` -- and the run exits **3**. Three rather than one: a
    wrapper must be able to tell "I could not read the fleet" from "I read it
    and something in it is wrong". Under `--json` the object carries
    `"complete": false` and the list of `timed_out_sections`.
*   The deadline is not the only way a census comes back short, and **3** is
    the answer to all of them. `complete` is true only when the deadline held,
    every required queue-root section (`pool`, `endings`, and the queue-root
    note that says which kind of empty an empty endings table is) read without
    raising, *and* the pool census and selected endings parsed every record they
    found. An unreadable ending keeps its diagnostic row and also appears in
    `unavailable_sections`; inaccessible terminal directories fail the endings
    section instead of silently contributing zero rows. Absent terminal
    directories remain compatible with a transport that has filed nothing. A section that
    raised is listed in `unavailable_sections` with its error class and text,
    which is kept apart from `timed_out_sections` because the two call for
    different next moves: a timeout says look at the mount, an error says look
    at the error. Before this, a prompt `PermissionError` on the queue root
    printed empty `nodes` and `jobs` under `"complete": true` and exited 0 --
    indistinguishable from a quiet fleet, which is the one confusion the flag
    was added to end.
*   What `complete` does not cover is SLURM reachability. `sinfo` or `squeue`
    missing or refusing is a statement about the scheduler, not about a census
    that could not be read; it is reported in the `scheduler` notes and still
    exits 0. Nor does a *stale* worker offer make a run incomplete: a box that
    stopped announcing was read correctly and is a fact about the fleet. Only
    a record nobody could read makes `pool.complete` false, and the records
    that could not be read are named in `pool.unreadable`. The scan for
    wedged `pbstatus` peers is outside the flag for the same reason: it is a
    diagnostic printed to stderr about this box, not a section of the census,
    so a scan that runs out of its slice of the budget says so on its own line
    and leaves the exit status alone.
*   `SIGKILL` is sent to an abandoned child and its exit verified within a
    short grace, because a timeout alone proves nothing about reaping. A child
    that does not exit is recorded in `abandoned_children` by PID *and*
    `starttime`, since a PID alone is reusable and therefore not an identity.
    SIGINT or SIGTERM aimed at the parent also closes its pipe and attempts
    the same bounded cleanup; surviving reader identities are printed to stderr
    even when cancellation prevents a JSON report. SIGKILL cannot run cleanup.
    At most one queue-root child is left behind per run: the shared budget
    means an expiry in one section leaves nothing for the next.
*   On start the run counts other `pbstatus` processes on the box already in
    uninterruptible sleep and prints one stderr line with the count, the oldest
    age and the PIDs. It never refuses to run on that count. A refusal would
    hide the fleet from the one person trying to see it, at exactly the moment
    it is worth seeing. The scan reads `/proc/<pid>/stat` for every candidate
    and `/proc/<pid>/cmdline` only for those already in `D`, because reading
    another task's command line takes that task's `mmap_read_lock` and a task
    blocked in an NFS page fault holds it; it is bounded by a PID count and a
    wall clock, and it runs in the same kind of abandonable child.

One thing the deadline does not cover, and cannot. When `pbstatus` is invoked
from the shared checkout, the interpreter reads the script and the
`prismabuild` package off the same mount before `main` exists. A mount sick
enough can block that, and no code inside the script can bound its own load.
The pool census phase is bounded; startup from a shared checkout is not.
The default transport metadata lookup after imports shares the census budget;
when it fails, the transport is unknown (`null` in JSON), never guessed.
Explicit SLURM status retains its separate per-command timeouts and an
unbounded lane-root lookup; the flag does not bound those scheduler reads.
Output writes and the short child-reaping grace are also outside the read budget. The
deadline also applies only to this diagnostic: it is not authority to time out
an action payload, which is the fleet's work and has its own contract.

Two flags say where `pbstatus` looks. `--lane-root` is the SLURM lane root that
job names are resolved against, and it defaults to `$PRISMABUILD_SLURM_LANE_ROOT`, or
to the fleet lane root when that is unset. `--queue-root` is the queue root
holding worker offers, ready/claimed state and the `done/` and `failed/`
endings. Pool status reads admission records beneath this same root rather
than probing a different host's local GPU.
Point them at a test fleet to read one without touching the live store.

The underlying commands are `sinfo` for nodes, `squeue` for jobs, and `sacct`
for jobs the controller has forgotten. Use them directly for scheduler detail
`pbstatus` does not join in.

### File the endings nobody asked for

Under SLURM the ending is filed by whoever polls for the key. A job that ends
while nothing is watching leaves its verdict in the controller's accounting and
never becomes a record. Nothing else fills the gap: `pbwait` would file it, but
only for a key somebody names, and the keys that need it are the ones nobody is
holding. A detached submission whose waiter died is the ordinary way to produce
one, and so is a `pool_reset` re-submission, which detaches on purpose.

`pbsweep` reconciles the lane against the queue and files what is missing:

    tools/fleet/pbsweep.py            # what is missing, and what would be filed
    tools/fleet/pbsweep.py --apply    # file it

Reporting is the default and writes nothing at all, so it is safe to run at any
time. `--apply` files each missing ending through the same call `pbwait` makes
for one key, so a swept record is the record a waiter would have written.

Three things it will not do. It does not invent a verdict: an ending is filed
only from the controller's terminal state, an operator's withdrawal marker, or
a receipt in the CAS. A job the controller knows nothing about with no receipt
behind it is reported `no-verdict` and files nothing, because a `failed` filed
on ignorance would stand for good. It does not replace an ending already filed,
for this generation or a later one. And it does not disturb a live `pbrun`
polling the same key: the two produce one record between them and neither
fails.

The table lists only keys that need attention. Its exit status is 0 when
everything was reconciled and 3 when some key could not be resolved, which is
distinct from 1, the code every fleet tool keeps for work that failed. `--json`
prints one object with a row per key and the counts. Name keys as arguments to
reconcile only those.

A malformed submission is reported as `unrecorded` without stopping the other
keys. A missing, invalid, or mismatched sealed request is `no-sealed-action`.
Withdrawal markers are enriched even when the controller still reports a live
job or has forgotten it. If the verdict disappears between the report and the
filing poll, `--apply` reports `no-verdict` and exits 3; it never reports an
unfiled prediction as the terminal status.

Terminal summary writers use permanent per-key POSIX lock files under
`pb-queue/.summary-locks/`. The supported shared filesystem is NFSv4 with
remote locking enabled (`local_lock=none`), including access through the
server's local filesystem. Do not delete these lock files while writers may
be active. The lock covers generation comparison and publication and is
released automatically when a process exits. An unreadable summary encountered
by a filer is preserved under its state directory's `unreadable/` subdirectory
before the known terminal record is written.

### Where the records live

| Location | What is there |
|---|---|
| `/mnt/shared/prismabuild-fleet/pb-queue/done/<key>.json` | The ending of an action whose work was done. |
| `.../pb-queue/failed/<key>.json` | The ending of an action with no receipt. |
| `.../pb-queue/withdrawn/<key>.json` | The marker for an action somebody cancelled. |
| `.../pb-queue/ready-transitions/<key>.<unix>.<id>.<kind>.json` | Original READY bytes moved aside for withdrawal or orphan examination. The key transition lock excludes concurrent mutation; an interrupted examination is retried by the reaper after the lease grace. Failed restores and unknown evidence remain here. Successful restoration never replaces a newer publication; final disposition preserves the bytes under `withdrawn/superseded/`. These captures own no resource reservation. |
| `.../pb-queue/claimed/<key>.<unix>.<host>.<pid>.<id>.tombstone` | A claim moved aside while its finisher publishes the action's next home. It exists for one write, and the finisher deletes it. One that outlives the lease timeout means the finisher was interrupted: the next reaper puts the record back as a claim, or, if the key already has a live or terminal record, files it under `withdrawn/superseded/` as evidence. A record whose attempt links no longer verify is filed the same way; one whose links cannot be *read* -- an unreadable or stale outcome blob -- is left where it is for the next sweep, because filing on an inability to look would retire a live action over a transient mount fault. No reader of `claimed/` counts it as a claim. |
| `.../slurm/<key>/` | The lane directory: `scripts/<sha256>.sh`, the immutable script each submission sent, plus `job.sh` as a pointer to the newest, `submissions/`, `latest.json`, `liveness.jsonl`, and `<jobid>.out` and `.err`. |
| `.../cas/` | The content-addressed store: action requests, results, and receipts. |

Both transports file their endings in the same two directories, so a SLURM
ending and a pull-queue ending appear side by side. The `schema` field says
which filed it: the two writers use distinct schema ids, and only the lane
writes a `transport` field. `pbstatus` labels its endings table from the schema
for that reason.

In the pull queue, a payload that has returned but whose scope cleanup is
still pending retains its claim, lease and reservation. The claim's
`finish_pending` field preserves the original status and detail, including
timeout or OOM evidence. The claiming host retries that finish on each queue
poll even while the lease is fresh; foreign hosts leave it alone. Once the
broker proves the exact attempt's scope empty, the original outcome is
archived once and capacity returns. A cleanup retry does not count as another
attempt or turn a completed action into a lease-loss failure.

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

## Reading the queue from an agent: the MCP server

An agent -- Claude Code, a Codex or opencode worker -- that wants to know what
its own submission is doing should not be shelling out to `pbstatus` and
parsing the table. The table is arranged for a person, its columns move when
the screen improves, and the sealed demand, the attempt history and the
receipt are not on it at all. `tools/fleet/pbmcp.py` answers the same
questions over MCP, as JSON, and does nothing else: it is read-only, every
read of the shared mount is deadline-bounded, and submission is deliberately
absent, because submission goes through `pbrun` and that is where the
permission hooks that gate it live.

Register it in Claude Code, from the directory the session runs in:

    claude mcp add --scope local prismabuild -- \
        /usr/bin/python3 /mnt/shared/prismabuild-fleet/repo/tools/fleet/pbmcp.py

and remove it with `claude mcp remove prismabuild`. For opencode or Codex, the
same launch as an `mcpServers` entry:

    {
      "mcpServers": {
        "prismabuild": {
          "command": "/usr/bin/python3",
          "args": ["/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbmcp.py"]
        }
      }
    }

Two things about that path. It is the published generation, not a checkout, so
a session always starts on the runtime the fleet is executing; and it is
`/usr/bin/python3`, because the server is stdlib-only and the boxes that most
need it are the ones with no venv and no checkout. `--queue-root`,
`--cas-root`, `--repo-link`, `--deadline-s` (default 5), `--recent` and
`--log-tail-bytes` are all available; the defaults are the fleet's own.

The tools:

*   **`pb_status`** — the `pbstatus` census: worker offers and their ages,
    ready and claimed actions with why each is waiting, per-host reservation
    token counts, and how the most recent actions ended.
*   **`pb_action(key_prefix)`** — one action, whole: the sealed submission
    (tags, demand, priority, checkout, `max_attempts`), its state and host,
    every attempt with its log metadata, the ending and both return codes, the
    CAS receipt, the derived local-result claim, and a tail of the last
    attempt's stdout. A prefix that names more than one action comes back with
    the candidates rather than a guess.
*   **`pb_actions(filter)`** — list by `states`, `tags`, `priority_min` /
    `priority_max`, `checkout_root`, `published_by`, `max_age_s` or explicit
    `keys`. This is "my jobs" for an agent, with one honest limit: a queue
    record carries **no submitter identity**. `publish` seals `published_by`
    (the submitting host) and either `checkout_root` or `checkout_snapshot`,
    and nothing that names an agent -- so an agent identifies its own work by
    the checkout it submitted from, the box it submitted on, or the keys it
    already holds. The answer says so in its `identity` field.
*   **`pb_verify_claim(sha256)`** — resolve the `local_result_claim_sha256` a
    run reported to its receipt and payload, reporting each check by name.
    `hash_payload: true` also reads and hashes the blob, which is off by
    default because a result blob is a rendered model often enough that
    hashing one by accident costs an hour of NFS bandwidth.
*   **`pb_log(key_prefix, tail_lines)`** — a bounded tail of one attempt's
    `stdout` or `stderr`. The log is never read whole: the reader seeks to the
    end and reads at most `--log-tail-bytes`.
*   **`pb_runtime()`** — the published generation, its manifest, which loops
    are announcing on it, and whether this server was started from it.

Every response carries the same envelope, and two of its fields decide whether
the rest of it can be believed. `complete` is false, and `timed_out` names the
section, when a read of the mount did not answer inside the deadline: an empty
`jobs` list with `complete: false` is a mount that did not answer, not a quiet
queue, and they call for opposite responses. `generation` and
`generation_stale` say whether `repo/` has moved since the session started --
after a publication the process is running code the fleet has replaced, which
nothing inside the process can fix, so it is stamped and the agent decides.
`generation_stale: null` means the link itself could not be read, which is
never the same as `false`.

Read-only is a property the tests hold it to rather than a promise: the module
has no mutating call in its import surface, every tool answers against a queue
root with its write bits removed, and every tool answers again with the
writing syscalls replaced by ones that raise. It also never expands an
attempt's logs -- `pool.attempt_outcomes` reads every stream whole to verify a
digest, which is right for a verifier and would make a status call on a
gigabyte of output cost a gigabyte.

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

On the pull queue, a withdrawal records the decision immediately and reports
`released 0` and `release pending on <host>` for a claimed action. The claiming
worker or reaper owns the claim, lease and reservation until it proves the
payload and containers stopped. The operator does not rewrite these records or
release capacity, even on the claiming host. A local broker request can speed
up termination using the exact saved scope identity. Otherwise the worker
checks the cancellation at its next heartbeat; withdrawal never searches for
processes by action key.

The stop request is immutable under
`withdrawn/decisions/<action_key>/<generation>.json`. A later submission retires
the visible withdrawal ending but cannot erase the old attempt's stop request
or cancel the new generation. A ready cancellation takes and re-reads its
record before removing it, preserving a concurrent replacement.

This replaces the legacy `max_attempts: 1` poison write and direct process
signalling. Publish with the normal drained upgrade so all workers read durable
generation decisions before relying on this contract. Older workers must be
upgraded; withdrawal does not fall back to mutating their claims or signalling
processes by name.

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

A pool worker receiving `SIGTERM` completes its current action and files the
outcome before exiting. This protects claims acquired after the supervisor's
idle check. Use the withdrawal command to cancel an action; worker rotation
is a request to drain. Supervisors also defer rotation when claim ownership
cannot be read, including the interval before a claim's first lease appears.

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

`--apply` starts each path-addressed re-submission with `pbrun --detach` and
waits for its structured admission acknowledgement and exit status. It prints
progress for slow submissions; elapsed time never counts as admission. A
refusal or missing/invalid acknowledgement leaves the original record `failed`
and makes the command exit non-zero after the rest of the batch is handled.
Successful acknowledgements, including cache hits and attachments to an
existing run, are saved under `reset.submission` with the new action key and
generation. The child exits after admission instead of streaming the action's
output. Its last 64 KiB of diagnostics stay under `<queue root>/resets/` for
operator inspection; each log is bounded, and old logs may be removed once
that evidence is no longer needed.

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
| `timeout` | The action was killed at a deadline. `returncode` is null, because an action that finished inside the tick that crossed the deadline would otherwise report 0 for a record filed as a timeout: read `status`, not `returncode`. Filed under `failed/`. Retriable. Under SLURM the deadline is the `--timeout-s` you asked for and the scheduler enforces it. The pool enforces the shorter of the sealed execution budget and the worker loop's own `--timeout-s` safety ceiling. |
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
    belong to another box. `pbrun` reads the source checkout to seal its bytes,
    so it can submit only for a checkout on the box it runs on. Submit from that
    box; the queue is shared, the filesystem is not.
*   **`cannot update pbrun Git excludes`** — initial exclude setup or migration
    cannot write Git's common `info/exclude`. Configure the checkout's excludes
    while that metadata is writable, then submit. Stamps and execution results
    do not require writes to the submitting tree.
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

The ladder dispatcher accepts `--wrapper /path/to/tessera_ladder_probe.py`.
Without it, the source is `tessera_ladder_probe.py` in the shared checkout;
there is no dependency on a submitting box's private Tessera tree. Each
submission records the resolved source path, SHA256, and staged relative path
in `params.wrapper_source`. The source bytes are staged under
`prismabuild-wrappers/<sha256>/tessera_ladder_probe.py`, included in the code
closure, and invoked at that relative path. Concurrent dispatches with different
wrappers keep separate copies. A conflicting existing digest path is refused.
The source path is provenance bound into the action key, so changing either
the source path or its bytes changes the key. `--dry-run --wrapper ...` previews
that same closure without staging files or publishing work.

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

The generation includes the execution skill, `docs/**/*.md`, `README.md` and
`AGENTS.md`. The skill's required policy and operating guide, and their linked
reference guides, therefore travel with the code they describe. Follow the
skill's relative document links inside that same resolved generation; no
mutable source checkout is needed to read the published execution contract.

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

A staged generation's import probe disables bytecode writes, so validation
does not add unlisted cache files before the generation is sealed.
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
*   Netdata reads it as its own UID. The [mount
    collector](mount_measurement.md) is installed as a symlink into the live
    generation on purpose, so that publishing a new runtime re-points it and no
    re-link is ever owed.

### What a published generation permits

A generation is world-readable: `0555` for the files Git records as executable
(`100755`), `0444` for the rest, and `0555` for its directories. Write is
denied to everyone, the owner included, so the property that makes a generation
quotable -- it is immutable, append-only history -- is unchanged.

Read is open because a published generation has a reader that is not `rob`.
The mount collector above runs as Netdata's own UID, and that UID is not in a
group you can rely on: on sparky it is 983 and in group `rob` only because a
`usermod` was run there, while on dl380g10 it is 984 with
`groups=984(netdata),110(docker)` and in no group of Rob's at all. Granting
only the group bit would therefore make plugin adoption depend on a per-box
`usermod` on every current and future box. The `other` bits need no per-box
provisioning. Nothing in a generation is secret: it is PrismaBuild's own
source, and credentials and queue state live elsewhere.

The mode is chosen by the publisher rather than inherited from the checkout it
copies. Before this, `shutil.copy2` carried the publishing worktree's mode into
the generation, so a worktree created under umask 077 published every file
`0500`/`0400` -- unreadable to the collector -- and the same commit published
from a umask-002 worktree would have published `0555`/`0444` (issue #316).
Which members are programs comes from the Git index, not from a filename or the
local filesystem, so the answer is the repository's and not the shell's.

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

## Sweep the store's per-execution litter

`pb_gc` inventories per-execution claims under `local-results/v1/`, worker lock
files, empty local-result staging namespaces, legacy root staging payloads,
and private `ingest.*` staging directories left by killed ingests. Requests,
receipts, unrecognized entries, and nonempty result staging namespaces are
retained. Normal ingest completion removes its private directory; SIGKILL can
leave payload bytes behind for this maintenance command to reclaim.

    tools/fleet/pb_gc.py --cas-root /mnt/shared/prismabuild-fleet/cas

The default only reports, including candidate paths, bytes, ages, reasons for
retention, and record counts. `--summary` omits individual paths. No default
CAS root is supplied. Candidate status describes local observations; it does
not establish that a remote execution is dead.

Before applying a sweep, stop new submissions and drain or stop **all CAS
producers on every host**, including direct clients outside the worker pool.
Review the dry-run paths and verify candidate checkout roots are absent on all
hosts where they could reside. Keep any claim needed to repair a persistent
checkout. Maintain this quiescence until the command exits. Only then run:

    tools/fleet/pb_gc.py --cas-root /mnt/shared/prismabuild-fleet/cas --apply --quiescent-store

Both flags are required for removal. `--quiescent-store` records the operator's
assertion; the tool does not acquire a distributed maintenance lock or stop
workers itself. A root missing on this host can exist on another host, and a
local `/proc` scan cannot see remote file users. Removing an unlocked worker
lock while producers may open it can leave a waiter on an orphan inode. The
maintenance requirement prevents relying on those incomplete local checks.

`--min-age-hours` is a finite, nonnegative retention threshold (default 24),
never proof of abandonment. Raising it does not make an online sweep safe.
The tool retains claims whose checkout roots exist or cannot be inspected,
locks protected by those claims or a local holder, occupied result staging
namespaces, open local staging files, and private ingest directories whose
ownership lock is held, missing, or contains unrecognized files. A private
ingest lock is held throughout copying and publication; the reaper also holds
it while deleting that directory. Incomplete hidden `.ingest.*` initialization
directories are retained for manual inspection.

A fresh survey and inode identity checks protect against entries that changed
since the report. Parent directories are opened without following symlinks.
A skipped removal exits 1 and reports the path; invalid arguments or an unsafe
store layout exit 2. These safeguards complement the maintenance prerequisite.
Use `core.repair_local_result` for occupied result namespaces: it takes the
output lock and validates the result's ownership before clearing a crash-left
publication.


## CPU tiers and container affinity

The live pool reserves physical performance cores before using SMT siblings
or efficiency cores. `--cpus` (or `--demand cpu=N`) declares the total CPU demand,
not a core-class preference. Small jobs use free preferred CPUs; wide jobs and
concurrent overflow can use the lower tier. Compatible free preferred capacity
on another host gets a bounded opportunity to claim work first. Constraints
still determine eligibility. Do not overdeclare CPU demand to force overflow.

Worker offers expose `cpu_tiers`; a claimed or terminal record's
`cpu_allocation` names its preferred and fallback CPU IDs. Children inherit the
reservation through `taskset`. The published Docker shim transfers it into
`docker run` and `docker create`; an explicit `--cpuset-cpus` is intersected
with the reservation. A disjoint mask or remote Docker context refuses with an
explanation. Use the action's ordinary `docker` command so the shim can preserve
CPU affinity and ownership labels. Directly choosing another Docker executable
or widening a child mask violates the agent execution policy.

The CPU map is immutable while a host serves work. To change an existing host's
usable topology or CPU cap: drain its reservations, stop its supervisor and
worker loops, preserve the old `reservations/<host>/cpu-map.json` as recovery
evidence, remove that map, then restart with the new shape. Never delete a map
while claims or loops can still use its CPU-token interpretation. Adding a new
host creates a separate map. Ordinary runtime publication with an unchanged
map uses the existing idle-queue procedure.

### Adaptive CPU admission

CPU samples, learned profiles, interval state and spent borrowing samples live
in the host-local `PRISMABUILD_BOX_STATE_ROOT` directory, keyed by ledger and
hostname. The default root is `/tmp/prismabuild-admission-<uid>`. Do not delete
it while workers run. A cold start relearns intervals and profiles; shared
copies are never recovery authority. For this authority migration or rollback,
keep the queue drained until every worker loop reports the selected generation.
Before rollback to shared authority, verify all snapshot publishers have
actually exited as well; a stalled publisher blocks that rollback.

Remote status readers still read `reservations/<host>/adaptive/`, now populated
by an independent publisher after admission is released. Its CPU record carries
`_snapshot.source=host-local`; the original sample timestamp governs freshness,
so a delayed copy stays stale even if it was just written. These are last
observations, not the host's current admission decision. A blocked copy cannot
hold admission and cannot spawn successors while it owns the separate local
publication flock. For an absent or stale copy, inspect `publisher-owner.json`
(PID, start ticks, nonce) and `publisher-result.json` beneath that ledger's
`<digest>.adaptive-cpu-v1` directory. Compare nonces before attributing a result.
No publication timeout establishes process exit or authorizes deleting locks.
Other shared claim operations remain exposed to a degraded mount.

The declared CPU demand remains an upper bound the action may actually use.
PrismaBuild measures current host CPU activity and pressure, including unrelated
processes, and combines that with consumption attributed to each running pool
attempt. Startup and any unaccounted interval are charged at the full declared
demand. Repeated executions of the same exact workload shape may establish a
conservative CPU-cost profile; a phase that consumes more CPU raises that cost
promptly, while old evidence decays slowly and expires.

Free preferred CPU tokens remain the first choice. When those are exhausted but
a running generation action has fresh, complete telemetry showing that it uses
less CPU than it reserved, the next generation action may share those reserved
preferred CPU IDs before taking free SMT siblings or efficiency cores. The same
evidence can support admission beyond the nominal physical-token count when the
box still has measured headroom. This is borrowing, not a smaller declaration:
continue to request the action's real peak CPU use.

Borrowing stops when host activity or CPU pressure reaches the admission bound,
when a donor becomes busy, or when any required observation is stale, malformed
or incomplete. Unknown startup work is protected at its full reservation, and
one host sample can authorize at most one new borrowing decision. Memory
reservations remain fully charged; CPU evidence never relaxes a memory budget or
authorizes GPU sharing. GPU admission uses its own broker evidence below.
An ordinary action may still acquire physically free tokens when per-attempt
telemetry cannot be read, but it receives no borrowing credit.

Measurements are stricter. They require a fresh nearly idle CPU observation,
do not share CPU reservations, and wait while another CPU action is held on the
host. Use the pool's platform-keyed default or explicit class placement, or SLURM's
explicit `--host-class CLASS` form described above. Use an exclusive GPU
reservation whenever competing GPU work would invalidate the result. GB10 GPU
utilization percentage is not a saturation measure; performance evidence should
include device power, CPU activity, residency and useful work over time.

Adaptive lending requires aggregate attempt telemetry for the complete execution
scope: the direct payload, descendants and daemon-created Docker containers.
Live pool workers use the resource broker to create an exact-attempt cgroup,
apply the action's memory ceiling, launch the payload inside it and attach owned
containers. The reservation is released only after the scope is empty. Missing
broker attachment, an unaccounted container or incomplete telemetry refuses
lending. A new worker needs broker installation and qualification before it can
join this execution path; runtime publication alone does not install the broker.

### Adaptive GPU admission and memory budgets

Each current GB10 worker advertises one physical GPU. Normal pool generation
work submitted with `--gpu` permits sharing; concurrency is chosen from fresh,
trusted broker observations rather than a per-host job-slot count. The broker
attributes all CUDA processes and residency to exact attempt scopes, and worker
loops share its snapshot. Several processes belonging to one action remain
that action's work. Missing, stale, incomplete or unattributed telemetry, or an
unknown memory domain, refuses even the first GPU claim; an unattributed process
blocks new work on the current single-device hosts.

With suitable device and host headroom, the pool admits one additional sharing
action at a time, waits for its activity response, and checks fresh observations
before expanding again. Power or thermal limits, host pressure and foreign work
stop new admission. If adding work produces no activity response above observed
noise, further probes pause until activity changes beyond that noise in either
direction or the busy period ends. Fresh headroom gates still govern admission.
Existing healthy actions keep running; the admission controller does not stop
them just because load rises. The separate memory guard may stop an exact
attempt that exceeds its budget or threatens shared memory.

`--exclusive` and GPU measurements do not share the device. Use `--measurement`
for performance results: the pool seals platform/toolchain identity and pins the
submitting host unless `--host-class` explicitly selects matching workers.
Optional SLURM measurements require `--host-class`;
use its `--exclusive` GPU reservation when overlapping work would invalidate a
result. Historical pool requests lacking explicit sharing intent remain
exclusive until they finish.

Declare GPU memory with pool `--gpu-memory-gb N`, a positive finite GiB value.
It requires GPU demand, is sealed into action identity, and does not rewrite
host `--demand mem_gb=M`. The two memory domains have different accounting:

| Memory domain | Budget contract |
|---|---|
| `shared_system` (GB10) | `mem_gb` covers aggregate physical DRAM used by the action. The GPU budget limits its GPU subset; omitting it defaults that cap to `mem_gb`. CPU and GPU allocation bounds are reconciled by the broker because CUDA allocations are not reliably charged to the cgroup. |
| `discrete` | Host RAM and VRAM are independent reservations. `mem_gb` limits host memory, while `--gpu-memory-gb` limits VRAM and defaults to `mem_gb` when omitted. Both budgets must fit; unused RAM does not provide VRAM capacity. |

For example, `--gpu --demand mem_gb=32 --gpu-memory-gb=8` reserves 32 GiB host
RAM and 8 GiB VRAM on a discrete device; on GB10 it permits 32 GiB aggregate
DRAM with an 8 GiB GPU subset cap. Missing VRAM counters refuse discrete GPU
admission. Unknown memory domains grant no capacity. `--gpu-memory-gb` is
refused with `--transport slurm`, whose separate VRAM enforcement is unsupported.

On GB10 the broker identifies its 140 W reference as SoC TDP, not a programmable
GPU-only power limit. It is an admission reference, not a saturation target.
GPU utilization percentage and an activity response alone do not prove useful
throughput; use profiling, power, CPU activity, residency and useful work per
joule when assessing a performance result.

### Elastic worker loops

The supervisor treats each box's `fleet_boxes.json` loop count as a floor. With
ready work and every current loop occupied, it starts a bounded batch of
additional queue pollers. A poller that remains idle shows that queue admission
has refused more work, so the supervisor does not keep multiplying processes.
After the ready backlog clears, it sends `SIGTERM` only to attributable loops
that a single claim census and the local process tree both prove idle; a loop
holding or launching an action is retained.

The automatic ceiling is derived from CPU affinity and visible memory and limits
only cheap housekeeping processes. It is not an action-concurrency setting and
does not replace adaptive admission. Busy or backlogged boxes are revisited on a
short bounded interval; idle boxes keep the ordinary interval. Spawns are
batched, and log indices are never reused, so contraction and later growth do
not mix two live workers' append evidence.

An idle loop with waiting work retries at a bounded one-second cadence so a
fresh adaptive CPU or GPU verdict is used promptly. With no ready work it
returns to the box's configured 10--20 second delay. The fast path reads the
shared broker snapshot and does not launch per-worker GPU probes.

No loop-count tuning is required for ordinary operation. `--loops N` is the
operator opt-out that fixes the count at `N`; `--once` retains deterministic
one-shot behavior and tops up only to the configured floor.

### Keeping a supervisor alive across a reboot

Each box runs its supervisor as a systemd **user** unit,
`prismabuild-supervisor.service`, installed by
`tools/fleet/install_supervisor_unit.sh` and enabled under the linger every box
already has. Install it as `rob`; it takes no `sudo`.

The unit runs the same `supervise.py --ensure` the crontab runs, and the
crontab line stays in place behind it. Neither can double up: one supervisor
per box is enforced by an exclusive `flock` on
`/home/rob/tmp/prismabuild-supervisor.claim`, and `--ensure` exits 0 quietly
when it loses that lock. What the unit adds is an owner. `cron` starts nothing
at boot and its finest useful granularity is minutes, so before the unit a
reboot left a box out of the pool until the next five-minute tick -- on
2026-09-06 all three boxes booted at 10:59 and rejoined at 11:05, with every
offer reading `stale` in between and nothing reporting it. `Restart=always`
with `RestartSec=30` makes that a thirty-second gap, and boot start makes it
seconds.

Two directives are load-bearing and neither is a default:

- `KillMode=process`. Worker loops are spawned by the supervisor and land in
  its cgroup, but they are not children to recycle -- a loop finishes its
  action under the generation that claimed it, and a replacement supervisor
  adopts the census instead of respawning. The default `control-group` would
  `SIGTERM` every loop mid-action on any restart of the unit.
- `StartLimitIntervalSec=0`, in `[Unit]`. The supervisor exits 0 by design in
  the `--ensure` no-op case and again when it re-execs onto a newly published
  generation, so no restart budget may retire the unit. The directive is
  honoured only in `[Unit]`; in `[Service]` systemd ignores it silently and the
  10s/5 default stays in force.

Installing the unit on a box whose supervisor is already running under `cron`
means handing the claim over. Stop the running supervisor by pid, using the
argv-shape search the runbook describes rather than `pkill -f`, then
`systemctl --user start prismabuild-supervisor.service`. The handover is
correct when the log's next line is `supervising N loops` with no `spawned
loop` lines after it: the new supervisor adopted every existing loop, and no
running action was disturbed.

## Export a complete Tessera model

Use `dispatch_tessera_model.py` for new full-model serving exports, including
single-file checkpoints. Supply the full source, plan, scales and immutable
producer identity; PrismaBuild selects whole-layer work quanta and runs the
receipt-gated merge. See [the model dispatcher guide](tessera_model_dispatch.md).
The legacy `dispatch_tessera_shards.py` remains the GLM input-shard interface.
