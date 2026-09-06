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
  --tag x86 --cpus 8 --demand mem_gb=8 -- \
  env PYTHONPATH=src OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 \
  /home/rob/venvs/pb-cpu/bin/python -m pytest -n 8 tests

python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --gpu --tag gb10 --cpus 4 --demand mem_gb=32 -- ./gpu-validation.sh
```

Use `pbtest.py` to split suites into independent file shards and
`pbcampaign.py` for explicit action manifests. Declare aggregate CPU and memory
use, bound native threads per subprocess, and specify GPU demand. Use tags for
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
