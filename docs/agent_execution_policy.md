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
inside containers. A measurement must retain its host-class identity and reserve
exclusive GPU capacity when competing work would invalidate the measurement.

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
