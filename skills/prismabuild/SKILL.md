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

GPU work must declare `--gpu` or the appropriate GPU demand through the client.
Use the PB Docker shim and preserve the admitted CPU mask and container parent.
Never replace the shim, widen affinity, or run directly over SSH to bypass a
queue. A CPU-only job must not turn GPU visibility back on.

The scheduler prefers physical performance cores and uses SMT siblings and
efficiency cores last. It may share lightly used CPU reservations based on
fresh measured demand, and stop admitting work as host pressure rises. Declare
honest peak CPU demand; do not inflate it to force overflow. Memory and GPU
reservations are not discounted by CPU oversubscription.

Use `--measurement` for measurements: the pool pins the submitting host and
seals platform/toolchain identity; SLURM measurements require `--host-class`.
Retain exclusive GPU capacity where competing work would invalidate results.
On GB10, GPU utilization percentage is not a saturation measure; collect power,
CPU activity, residency and useful throughput with before/after profiling.

## Verify and recover

A submission acknowledgement is not completion. With `--detach`, retain the
action key and use published `pbwait.py`/`pbstatus.py` to inspect the terminal
state. Check exit status, actual logs and the CAS receipt/payload. Record test
counts, skips, devices and missing tooling; do not certify a wrapper's “done”.

If PB is unavailable, diagnose and repair it instead of bypassing admission.
An OOM ending should name its exact attempt and memory evidence. Do not kill
processes by a loose name match or release held tokens while owned work lives.
Runtime and locally installed client versions should be checked through the
published status and upgrade evidence, not inferred from the checkout's HEAD.

Read the repository's `docs/agent_execution_policy.md` and
`docs/operating_prismabuild.md` for campaign/recovery contracts. This skill is
execution guidance, not an OS access-control boundary or new authorization to
change administrative privileges.
