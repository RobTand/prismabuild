# Agent execution through PrismaBuild

Rob requires all agent and subagent test execution and GPU work to run through
PrismaBuild. This applies across projects and hosts, including targeted tests,
full suites, benchmarks, builds that execute GPU kernels, and validation inside
containers. The coordinator submits; a worker executes after admission. Tests
and subprocesses already inside an admitted action do not submit themselves
recursively.

Use the published runtime at `/mnt/shared/prismabuild-fleet/repo/tools`:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --tag x86 --cpus 4 --demand mem_gb=8 -- \
  env PYTHONPATH=src OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 \
  /home/rob/venvs/pb-cpu/bin/python -m pytest -n 4 tests

python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --gpu --tag gb10 --cpus 4 --demand mem_gb=32 -- ./gpu-validation.sh
```

Submit from the published runtime, not from a worktree copy of `pbrun.py`.
`pbrun` transports the *checkout* through the CAS, so a checkout under
`/home/rob/...` runs on any box -- but it does not transport itself. It seals
its own `tools/prismabuild_worker.py` as an absolute path into the action, and
out of a worktree that path exists on one box. `pbrun` therefore refuses at
submit when the placement admits a box that could not open the runtime, and
says which box has it; add `--tag <thisbox>` (or `--here`) if you meant to keep
the work here, or submit through the published runtime if you did not. This is
also why `pbtest` names no default tag when run from a worktree: the `x86`
default is an explicit placement claim, and an explicit tag outranks the pin
`pbrun` would otherwise derive (RobTand/prismabuild#292).

Every worker loop kills an action at its own safety ceiling, **7200 s by
default**, and that ceiling is applied as a silent `min` against `--timeout-s`.
Each box now announces its ceiling (`pbstatus` shows it as `KILL AT`), `pbrun`
says at submit when `--timeout-s` asks for more than an eligible box will
grant, and the receipt records what actually governed
(`execution_timeout_s`, `execution_timeout_requested_s`,
`execution_timeout_ceiling_s`, `execution_timeout_clamped`). A job that needs
longer than the ceiling needs a loop started with a larger `--timeout-s`, not
a larger `--timeout-s` on the submission (RobTand/prismabuild#293).

An action that can say when it commits work need not be bounded by elapsed
time at all. Declare the phases it walks and the quiet each one is allowed --
`pbrun --progress-phase startup=1800 --progress-phase encode=900`, or a
`progress_phases` list in a `pbcampaign` manifest -- and the action reports
each commitment by writing `prismabuild.action_progress.v1` to the file named
by `PRISMABUILD_ACTION_PROGRESS_PATH`, echoing `PRISMABUILD_ACTION_PROGRESS_TOKEN`
(`prismabuild.report_action_progress(phase, units_completed)` does this).
What then bounds it:

* **no total-duration limit while the count advances.** The worker's ceiling
  clamps each phase's allowance instead of the whole run.
* **the sum of the declared allowances** if it never advances. Each phase
  re-arms once, in the order declared, so that sum is a number the receipt
  reports (`progress_no_progress_bound_s`) rather than a constant somebody
  chose.
* **`--timeout-s` still ends it**, progress or no progress. Precedence is
  containment, withdrawal, the requested deadline, then the stall allowance.

Advancement is a strictly increasing cumulative `units_completed` across the
whole action, or entering a later declared phase. The count starts at zero;
reporting zero in the initial phase does not renew startup grace.
A replayed or regressing counter, an
undeclared phase, a token from another attempt, printed output and a live
process are **not** advancement; the receipt says which
(`progress_observation.last_rejection`). Report after the work is durable --
a checkpoint written, a unit published -- never on entering a loop, or the
counter keeps a broken action alive. A stall ends the action with
`status: timeout` and `termination_reason: no_progress`; a requested deadline
with `termination_reason: execution_deadline`.

Choose the allowances from what the workload measurably does. PrismaQuant's
pricing rows declare `startup=3600 pricing=900 finalize=1800` because a fit of
elapsed time against committed batches over 23 completed 864-unit rows gives
18.6728 s per batch, an 836.1 s intercept and a 160.5 s maximum absolute
residual. The [retained records and extraction](https://github.com/RobTand/prismaquant/blob/83fa2a2dfee478a68a4e582764932c21b38e7747/docs/measurements/pq480_progress_grace_fit_2026-09-10.md)
make that workload-specific fit reproducible. The intercept estimates time
outside pricing; it does not directly measure each phase's longest quiet gap.
The allowances add margin to that evidence. Their 6,300 s sum is less than
half the 14,400 s that killed two rows mid-round (RobTand/prismabuild#480). The watcher uses the existing stable
regular-file reader at heartbeat cadence, with a 64 KiB accepted-byte cap and
strict UTF-8 JSON. Invalid, duplicate-key, oversized, symlink and FIFO reports
do not renew grace. The reader bounds bytes and retries; a kernel-blocked NFS
operation has the same recovery limitation as lease/withdrawal I/O (#16).

`pbrun` **refuses** a progress-declaring submission when no eligible worker
announces the contract (`pbstatus` shows what each box announces). That is
deliberate, and stricter than the ceiling notice above: a box that does not
know the policy would apply its whole-run ceiling to an action submitted
without one, which is the failure the contract exists to remove. On a mixed
fleet the submission is narrowed instead: it requires the `progress-v1` tag
that only an upgraded worker offers, so an old box cannot claim it, and the
notice names the boxes being waited past.

Two more refusals, both fail-closed. `--progress-phase` requires the pool
transport -- the watchdog is the pull-queue worker's, and SLURM can enforce
only a total duration -- and an action that declares a policy refuses to
launch if the launcher gave it no channel, rather than running with nothing
bounding it.

`pbstatus`'s job table has a `PROGRESS` column beside `OUTPUT`: the quiet time
against the current phase's allowance, and how many reports have been accepted.
`OUTPUT` age is log traffic and proves nothing; `PROGRESS` is what the watchdog
acts on. A blank column means no valid progress observation is available; it
alone does not identify the action's execution policy. Read the sealed request
and terminal policy fields to distinguish a missing observation from an action
with no declared progress policy.

Use `pbtest.py` to split suites into independent file shards and
`pbcampaign.py` for explicit action manifests. Cap pytest fanout at `-n 4` with
one native thread per worker (`OMP_NUM_THREADS=1`, `MKL_NUM_THREADS=1`,
`OPENBLAS_NUM_THREADS=1`) and one reserved CPU per worker. That cap is the
fleet's operating limit while several agents submit concurrently, not a property
of the tool: a wider fanout multiplies small-file traffic against the shared
`/mnt/shared` mount, and RobTand/prismabuild#217 records the contention that
follows. Raise it only against a measurement showing the mount has room.

Declare aggregate CPU and memory use, bound native threads per subprocess, and
specify GPU demand. Use tags for actual dependencies and architecture, allowing
any eligible worker to claim portable work. Reserve the CPU count the workload
actually uses; do not inflate reservations to force access to additional cores.
Physical performance cores are the preferred tier; SMT siblings and efficiency
cores are overflow capacity. The pool selects CPUs, and agents must preserve its
assigned affinity, including inside containers. A pool measurement defaults to
the submitting host and its platform/toolchain identity. Explicit
`--measurement --host-class CLASS` permits matching workers when the complete
experiment and external dependencies are identical across the class. The worker
verifies platform/ABI, shell executable, driver and device models; each paired
experiment stays in one isolated action. `--anywhere` is invalid. A SLURM
measurement must retain its explicit host-class
identity. Reserve exclusive GPU capacity when competing work would invalidate
either kind of measurement.

CPU admission is adaptive, so a declared CPU count remains the action's honest
peak demand rather than a promise that every reserved core will stay busy. The
pool uses free preferred cores first. Before it uses fallback cores, it may lend
freshly attributed, lightly used preferred capacity from another generation
action; it may also admit beyond the physical token count when the same evidence
and current host headroom support it. Host CPU use and pressure include work
outside PrismaBuild and can stop further admission. Missing, stale or incomplete
attempt telemetry grants no lending credit. Memory demand is never discounted. GPU concurrency uses its own trusted device
admission policy rather than CPU lending; measurements do not borrow CPU
capacity or overlap another
admitted CPU action. On GB10, GPU utilization percentage is not evidence of
saturation; use power, host activity, residency and useful throughput for a
performance claim.

GPU admission uses physical devices and attributed broker telemetry rather
than a manually tuned job-slot count. Each current GB10 has one physical GPU.
Missing, stale, incomplete or unattributed telemetry admits no GPU work;
multiple CUDA processes belonging to one verified action remain that action's
work, while an unattributed GPU process blocks another claim. Host `mem_gb` and
discrete VRAM budgets are separate. Worker loops share the broker snapshot and
retry a nonempty queue promptly instead of running their own GPU probes.

Per-attempt telemetry must cover the whole execution scope, including direct
children and daemon-created containers, before adaptive CPU lending is enabled
on a worker. The privileged scope broker is the architectural boundary for that
accounting, memory containment and exact-attempt termination. If the broker,
scope attachment or aggregate telemetry is unavailable or ambiguous, the worker
must fail closed for lending and must not treat partial process telemetry as the
job's consumption. This requirement is not itself a claim that broker deployment
or cross-host qualification has completed.

Check the exit status, terminal record, logs and CAS receipt. A submission
acknowledgement is not a passing test. Record skips and missing tooling. Install
scoped tooling where required instead of silently skipping qualification.

Read-only inspection, source edits, Git operations and submission commands may
run on the coordinator. Do not hide tests in SSH commands, shell scripts,
containers, inline Python or another agent to evade admission. If PrismaBuild
cannot admit work, diagnose and repair its availability; do not silently fall
back to untracked local execution. A bootstrap exception requires an explicit
user instruction and its scope and evidence must be recorded.

## Measure wall time

Submit every wall-time measurement with `--measurement`. Do not assemble a quiet
window by hand, and do not reach for `--exclusive` instead: `--exclusive` asks
for one box's whole GPU capacity, so it derives GPU demand and refuses when no
tagged worker announces a GPU. It says nothing about CPU isolation.

`--measurement` is the isolation mechanism, and `adaptive_cpu.Controller.decision`
enforces it. A measurement is admitted only against a fresh host sample showing
at most 5% of the box's cores busy. It refuses to start while any other
reservation is held on the host, and blocks other admissions while it holds one.
It never borrows CPU capacity, and its own reservation is never lent out. Action
validation in `core` refuses a measurement whose execution scope is portable, so
the result carries the platform or host class that produced it.

Interleave the arms: before, after, before, after, rather than every repeat of
one arm followed by every repeat of the other. Interleaving cancels background
drift instead of accumulating it into whichever arm ran later.

Record the observed load at the start and end of every arm, and report it beside
the timings. If the spread between repeats of the same arm is comparable to the
gap between the arms, the comparison is inconclusive. Report it as inconclusive
rather than reporting a mean.

Expect a `--measurement` action to wait. Its near-idle precondition is
unsatisfiable while the fleet is busy, which is when you most want to measure, so
the action can sit `READY` for a long time. The precondition also has a
freshness half: a host sample older than a few seconds fails it as surely as a
busy box, so stale worker telemetry holds a measurement back on an idle fleet.
RobTand/prismabuild#205 tracks that staleness. Wait for the action, or measure
when the fleet is quiet. Dropping `--measurement` to get admitted yields a number
that describes the fleet rather than the change.

## vLLM is exempt; its load is not

vLLM is exempt from submission, universally. Anything that runs it -- a serve, a
census, a routing run, a benchmark against a live endpoint, its GPU containers --
runs directly, without a PrismaBuild action and without bootstrap approval.

The reason is Rob's ruling, not a property PrismaBuild could measure. Rob,
2026-09-06: *"everything vllm is exempt from everything. Vllm isn't something
that uses chunks of work, it's an llm serving runtime."* Rob, 2026-09-07: *"vllm
is exempt. It can't run in prismabuild. This is not complex don't make it so."*
A bounded action that happens to start vLLM can run to completion under
PrismaBuild -- action `a475cb59` did -- and it is exempt all the same. The
exemption turns on whether the thing runs vLLM, not on whether a given run
feels like a test or divides into quanta.

This does not exempt the work *around* it. A test that does not run vLLM still
submits, and so does an export, a probe or a cost stage that merely runs on the
same box.

A directly started serve remains **external load for batch admission**. It is
outside the ledger, so no reservation accounts for the CPU, memory and GPU it
holds, and a box that looks idle in the queue view can be fully committed to it.
Read a serve's own resource use before concluding that admission is at fault.

## Persistent instructions and command guard

The same requirement belongs in each host's global agent instruction files
and in delegation briefs. Repository `AGENTS.md` links this policy. The
`require_pool.py` hook rejects recognized off-pool test runners, GPU interpreter
and container launches, and direct scheduler submissions when
`/home/rob/tmp/arb/require_pool.on` exists. Claude Code installs it as a
`PreToolUse` Bash hook. Other agents must follow their global instructions;
this command parser is a guard, not an operating-system security boundary or a
proof of arbitrary program behavior.

It refuses instances of that work, not mentions of it. A quoted argument to an
interpreter running a script file -- a message body, a subject line -- is prose
and is not scanned; anything that executes an argument, including `python -c`,
`python -m`, a shell, a wrapper such as `ssh` or `docker`, and a launcher that
forwards its trailing argv, is scanned as before. Prose the lexical reader
would still misread, such as a body carrying an escaped quote, belongs in a
file argument rather than on the command line.

## Adding workers

Mount the same CAS/queue at `/mnt/shared`, provision the declared architecture's
interpreter and dependencies, and add one explicit host entry (or unique rename
alias) in `tools/fleet/fleet_boxes.json`. Declare measured CPU, memory and GPU
capacity, publish a validated generation with an idle queue, then start its
supervisor on the new host. Verify a fresh offer and a real admitted CPU action,
and a GPU action for GPU workers, including their CAS receipts. Install the
same global policy and hook before using agents on the new worker. Expand
capacity through these offers; agents must not invent an independent queue or
bypass resource reservations.

GPU admission requires fresh broker evidence even for its first action. Both GB10
workers advertise one physical device and use the same adaptive sharing policy.
`--exclusive` and measurements remain exclusive. On discrete GPUs, declare the
separate VRAM budget with pool `--gpu-memory-gb N`; it defaults conservatively
to `mem_gb`. Host RAM and VRAM are reserved independently. On GB10, `mem_gb`
remains the shared physical budget and an explicit GPU budget is a subset cap.
Unknown memory domains or missing counters grant no admission credit.
