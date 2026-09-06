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

Use `pbtest.py` to split suites into independent file shards and
`pbcampaign.py` for explicit action manifests. Cap pytest fanout at `-n 4` with
one native thread per worker (`OMP_NUM_THREADS=1`, `MKL_NUM_THREADS=1`,
`OPENBLAS_NUM_THREADS=1`) and one reserved CPU per worker. That cap is the
fleet's operating limit while several agents submit concurrently, not a property
of the tool: a wider fanout multiplies small-file traffic against the shared
`/mnt/shared` mount, and RobTand/prismabuild#217 records the contention that
follows. Raise it only against a measurement showing the mount has room.

Declare aggregate CPU and memory use, bound native threads per subprocess, and specify GPU demand. Use tags for
actual dependencies and architecture, allowing any eligible worker to claim
portable work. Reserve the CPU count the workload actually uses; do not inflate
reservations to force access to additional cores. Physical performance cores are
the preferred tier; SMT siblings and efficiency cores are overflow capacity.
The pool selects CPUs, and agents must preserve its assigned affinity, including
inside containers. A pool measurement implicitly retains the submitting host
and its platform/toolchain identity; `--anywhere` is invalid for it. A SLURM
measurement must retain its explicit host-class identity. Reserve exclusive GPU
capacity when competing work would invalidate either kind of measurement.

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

## Persistent instructions and command guard

The same requirement belongs in each host's global agent instruction files
and in delegation briefs. Repository `AGENTS.md` links this policy. The
`require_pool.py` hook rejects recognized off-pool test runners, GPU interpreter
and container launches, and direct scheduler submissions when
`/home/rob/tmp/arb/require_pool.on` exists. Claude Code installs it as a
`PreToolUse` Bash hook. Other agents must follow their global instructions;
this command parser is a guard, not an operating-system security boundary or a
proof of arbitrary program behavior.

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
