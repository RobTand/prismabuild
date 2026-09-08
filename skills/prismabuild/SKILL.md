---
name: prismabuild
description: Submit tests, benchmarks, builds and GPU work through the PrismaBuild fleet; size admitted work and verify its actual result. Use for agent and subagent test or GPU execution on Rob's workers.
---

# PrismaBuild execution

Rob requires every agent and subagent to route tests and GPU work through PB.
Use the published client at `/mnt/shared/prismabuild-fleet/repo/tools/` so a
stale checkout does not become a stale submission client. Read-only inspection,
source edits and Git operations may run locally. Children already executing
inside an admitted job run directly; do not recursively submit them.

## Tiny work quanta; PrismaBuild owns distribution

Rob's explicit instruction (2026-09-05): supply enough useful GPU work for the
fleet to balance it, and make work available as tiny, independently verifiable
and retryable quanta. Prefer the smallest useful units that preserve required
calibration identity, residency, dependency order and measurement isolation;
avoid opaque long-running wrappers around otherwise independent work.

Use PrismaBuild's supported fanout and campaign interfaces. Give `pbtest.py`
the suite and let it partition tests; give `pbcampaign.py` independent logical
actions, and submit dependent work only after its inputs are complete. Keep
enough authorized, runnable work queued to occupy all eligible GPUs and CPUs.
Choose granularity so startup and transfer overhead do not overwhelm useful
work; use actual receipts and telemetry to assess it.

Sharding, distribution, placement and balancing are solely PrismaBuild's job.
Agents must not manually divide test files, layers or tensors among hosts,
write a second dispatcher, assign a GPU job to Sparky just to light it up, or
invent shard/worker counts in application code. Declare real dependencies and
resource requirements through PB; leave eligible-worker selection to PB. If
PB lacks a needed subdivision capability, report that concrete gap in PB
instead of building an application-side workaround.

When a device is idle, inspect PB's ready queue, active claims and admission
reasons. Distinguish insufficient runnable work from an admission failure or
a status-view defect. An empty queue is a work-supply gap; do not manufacture
load, duplicate completed measurements, weaken gates or bypass isolation.

## Submit work

`pbrun.py --cwd CHECKOUT` snapshots the checkout, including relevant dirty work,
and executes that snapshot on an eligible worker. `--cwd` belongs to pbrun;
`--checkout` belongs to pbtest. Inspect the published tool's `--help` for
options rather than inventing aliases.

Example CPU suite, sized for eight processes and eight GiB total:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --cwd /path/to/project --cpus 8 --demand mem_gb=8 \
  --env OMP_NUM_THREADS=1 --env MKL_NUM_THREADS=1 \
  --env OPENBLAS_NUM_THREADS=1 -- \
  /home/rob/venvs/pb-cpu/bin/python -m pytest -n 8 tests/
```

Choose CPU/memory demand for the actual combined workload; the example is not a
universal setting. Use `pbtest.py` for suite fanout and `pbcampaign.py` for a
manifest of independent actions. Prefer portable placement; add a host tag only
for a real dependency or a controlled measurement.

`--profile sample` runs py-spy around the action's child at 100 Hz and files
the speedscope profile as a CAS blob whose digest and path appear on the
ending, for `pbrun`, `pbstatus` and a human with speedscope. Reach for it when
you need to know where an action's time went, not routinely: unlike
`--priority` it **is** sealed into the action key, so a profiled run is a
different action -- never a cache hit for the unprofiled one, and never an A/B
arm against an unprofiled receipt. Measured overhead on a fixed-work CPU action is
in `docs/operating_prismabuild.md`, beside the box load it was measured under. A profiled action whose profiler produced nothing fails
with the reason rather than returning an unprofiled receipt, so do not use it
on an action too short for a sampler to see. An action killed by its deadline still files whatever
profile it had reached, marked `partial`, and a profiled failure carries the
same `returncode`/`signal` an unprofiled one would. `pbtest.py --profile
sample` and a manifest row's `profile` field forward it. It is backed on dl380g10 and
sparklina and refuses on sparky, whose worker loop launches under an
interpreter that cannot see py-spy.

`--profile nsys` and `--profile torch` are the GPU modes, for an action that is
slower than it should be when you do not yet know where. `nsys` runs Nsight
Systems over CUDA and NVTX and files the `.nsys-rep` plus its kernel-time CSV;
it is backed on both GB10 boxes and refuses on dl380g10, and `nsys:600` traces
a window in seconds without ending the action. `torch` is a contract rather
than a wrapper -- PrismaBuild names a path in `PRISMABUILD_PROFILE_TORCH_OUT`
and the action exports its Chrome trace there (copy `tools/profile_torch.py`);
an action that ignores it fails rather than filing a profile-less receipt.
Measured cost on a fixed-work GPU action, five interleaved paired repeats, is
in `docs/operating_prismabuild.md`; both are dearer than `sample`, and most of
it is fixed startup rather than a tax on the work.

Agent self-validation -- test shards, the receipt for a PR, a re-run to confirm
a fix -- submits at `--priority -10` (`pbtest.py --priority -10`, `pbrun.py
--priority -10`). Ready items are ordered by priority band before aging, so
that work is considered only after everything at the default priority has been
tried and never displaces campaign work in the queue. Campaign work stays at 0.

An explicitly retry-safe `-10` generation action can also yield the box: when a
foreground item is denied for tokens a background holder is sitting on, that holder is
withdrawn through the withdrawal ladder and re-published at `-10` with its aging
count, provided an attempt remains after this interruption. Each preemption
consumes one of the original `max_attempts`; it never resets the budget.
Measurements, unknown actions and holders with recorded failures remain running.
Use `--retry-safe` with a suitable bounded `--max-attempts` only for commands
that can safely restart after interruption. Priority alone grants no restart
permission. An eligible agent shard can be stopped when campaign work arrives;
read `preempted_by` on the requeued row rather than treating the interruption as
a failure. A `pbrun` or `pbtest` that is waiting does not exit on the stop: it
follows the requeue and reports the retry's result, so a preempted shard costs
one attempt and wall-clock time, without producing a failure verdict itself.
The stop is not instant -- the holder returns its tokens at its next checkpoint -- so shards that are short still cost the
campaign least.

GPU work declares `--gpu` or the appropriate GPU demand. The live pool shares
one physical GPU among generation actions when fresh broker observations show
headroom; let admission choose concurrency rather than tuning GPU job slots.
Missing, stale, incomplete or unattributed telemetry defers GPU admission,
including the first action. Check the broker/worker evidence when work waits.

Use pool `--gpu-memory-gb N` for the GPU memory budget in GiB. On a discrete
device, it reserves VRAM independently of host `--demand mem_gb=M`; both must
fit. On GB10 (`shared_system`), `mem_gb` covers total physical DRAM and the GPU
budget caps its GPU subset. The GPU cap defaults to `mem_gb` when omitted.
The option requires GPU demand and is unsupported with `--transport slurm`.

The live pool contains each attempt's payload, descendants and Docker
containers in a broker-owned scope. Use the ordinary `docker` command so PB's
shim preserves its CPU mask and container parent. Keep children inside that
scope and retain CPU-only jobs' disabled GPU visibility.

The scheduler prefers physical performance cores and uses SMT siblings and
efficiency cores last. It may share lightly used CPU reservations based on
fresh measured demand, and stop admitting work as host pressure rises. Declare
honest peak CPU demand. Memory budgets remain fully reserved; CPU lending does
not authorize GPU sharing.

Use `--measurement` for measurements: the pool defaults to the submitting host,
seals platform/toolchain identity, admits only against a fresh near-idle host, and
keeps GPU measurements exclusive. Interleave the arms of a timing comparison and
record the load per arm; see `docs/agent_execution_policy.md`. Explicit pool
`--measurement --host-class CLASS` lets PB select a matching worker when the
complete paired experiment and external dependencies are identical across that
class. Platform/ABI, shell executable, driver and GPU models/counts are verified;
actual worker and GPU UUID stay in the receipt. Keep both arms in one admitted
action and pin/record inner container or Python dependencies in the experiment.
This is placement eligibility, not an isolation exemption. `--exclusive`
reserves one box's whole GPU capacity and is not CPU isolation; it also prevents
GPU sharing for ordinary work. Optional SLURM measurements require
`--host-class`; reserve its GPU exclusively when overlap would invalidate results.
On GB10, GPU utilization percentage is not a saturation measure; collect power,
CPU activity, residency and useful throughput with before/after profiling.

vLLM is exempt from submission, universally: anything that runs it -- a serve, a
census, a routing run, a benchmark against a live endpoint -- runs directly,
including its GPU containers. That is Rob's ruling (2026-09-06, reaffirmed
2026-09-07: "vllm is exempt. It can't run in prismabuild"), not a claim about
what a wrapper could technically do -- a bounded action that starts vLLM can
exit cleanly, and is exempt all the same. Work that does not run vLLM still
submits, and a running vLLM remains external load for batch admission.
`docs/agent_execution_policy.md` carries the full ruling.

## Verify and recover

A submission acknowledgement is not completion. With `--detach`, retain the
action key and use published `pbwait.py`/`pbstatus.py` to inspect the terminal
state. Check exit status, actual logs and the CAS receipt/payload. Record test
counts, skips, devices and missing tooling; do not certify a wrapper's “done”.

Read status and your own actions as data, not as text. Register the read-only
MCP server -- `claude mcp add --scope local prismabuild -- /usr/bin/python3
/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbmcp.py`, or the same command
as an `mcpServers` entry for opencode/Codex -- and use `pb_status`,
`pb_action`, `pb_actions`, `pb_log`, `pb_verify_claim` and `pb_runtime`
instead of shelling out to `pbstatus` and parsing its table, which is arranged
for a person and whose columns move. Filter `pb_actions` by `checkout_root`,
`published_by` or explicit `keys`: a queue record carries no submitter
identity, so those are what identify your own work. Read `complete` and
`timed_out` on every response before believing it -- a section that did not
answer comes back as `null`, never as an empty list, and a read that failed
outright is named in `unavailable`. `pb_verify_claim` answers with
`checks_passed`, not `verified`: `attestation_verified` is `null` because that
check needs the full action manifest. The
server never writes and cannot submit; submission stays with `pbrun`.

Every ending carries `detail.resource_profile`, so what a run cost is part of
what you check rather than something to re-measure: `reaped_children` (the
parent's rusage around the launch, whose `scope` field says it covers the
launcher on a contained run), `scope` (the attempt's cgroup — `memory_peak_bytes`
and the user/system CPU split), `process_io` (`rchar`/`wchar` and
`read_bytes`/`write_bytes` over the processes in the scope) and `box_window`
(GPU power against the device's own reference, unified memory, CPU busy and the
pressure stalls, from `pqteld` and Netdata). It is always on and is not a profile mode: there is
no flag to choose. Its cost is bounded rather than absent — a shared two-second
budget at finish to read the box window, including the GPU reference query
(0.14–0.18 s measured before the query shared that budget), plus a sampler that
ticks at most every two seconds while the attempt runs. It never enters the
action key. The deadline is cooperative: filesystem reads and process cleanup
can overrun it. An absent field means the source recorded nothing;
it does not mean zero. `pbstatus` shows it in the `RESOURCE` column and
`pbmetrics` exports it.

If PB is unavailable, diagnose and repair it instead of bypassing admission.
An OOM ending should name its exact attempt and memory evidence. Do not kill
processes by a loose name match or release held tokens while owned work lives.
Runtime and locally installed client versions should be checked through the
published status and upgrade evidence, not inferred from the checkout's HEAD.

Read the [execution policy](../../docs/agent_execution_policy.md) and
[operating guide](../../docs/operating_prismabuild.md) for campaign/recovery
contracts. These relative links resolve inside the same sealed generation as
this skill; use that generation's documents rather than a mutable checkout.
This skill is execution guidance, not an OS access-control boundary or new
authorization to change administrative privileges.
