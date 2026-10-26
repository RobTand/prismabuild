---
name: verify-prismabuild
description: Verify PrismaBuild CLI changes through fleet test tiers, inspect receipt-backed verdicts, and qualify real NVIDIA execution. Use before handing a PrismaBuild change to review.
---

# Verify PrismaBuild

Use the [feature map](features/README.md) to select the affected user path. Read the repository's [execution policy](../../../docs/agent_execution_policy.md) before running tests. The commands below submit through the published client. Children already inside an admitted action run pytest directly, without calling this submitter again.

## Launch

PrismaBuild is a short-lived CLI. Reuse the published runtime; this verification does not start worker services or install packages.

From the checkout root, create an evidence directory outside the checkout:

```bash
PB_EVIDENCE=$(mktemp -d /tmp/pb-verification.XXXXXXXX)
```

The default worker environments are `/home/rob/venvs/pb-cpu/bin/python` for CPU and `/home/rob/venvs/pq-cu130/bin/python` for GPU. They need not exist on the coordinator. Override `--python /absolute/worker/python` when a different qualified worker environment is required. Each run prints its evidence directory and submitted command before execution.

## Doctor

Capture a read-only fleet observation:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbstatus.py --json > "$PB_EVIDENCE/fleet.json"
```

Check `complete`, `timed_out_sections`, `unavailable_sections`, and the live `nodes`. A stale or incomplete observation is inconclusive. Confirm the published `repo/RUNTIME_VERSION.json` generation rather than inferring deployment from this checkout's HEAD. If the fleet cannot admit the work, inspect its denial and repair the actual cause under the execution policy.

## Drive

Run each tier with one command from the checkout root:

```bash
python3 tools/verify.py fast --report-dir "$PB_EVIDENCE/fast"
python3 tools/verify.py integration --report-dir "$PB_EVIDENCE/integration"
python3 tools/verify.py gpu --report-dir "$PB_EVIDENCE/gpu"
```

For an ordinary sample change, run `fast`, then select the affected feature from the map. `fast` is the unit gate and must finish in less than 600 seconds, including submission and receipt verification. `integration` covers every other CPU test file, including newly added tests. `gpu` is the NVIDIA qualification command and refuses a skipped hardware check. A fast pass alone does not qualify GPU execution or all integration behavior.

Use `python3 tools/verify.py TIER --plan` to inspect the selection and command without submitting. Plan mode reads source paths and starts no tests, pytest collection, network request, or Git mutation.

PB chooses shard membership and placement. The tier command reserves one CPU and four GiB per CPU shard, two CPUs for the integration two-CPU lane, or one CPU, eight GiB host RAM and two GiB GPU memory per GPU shard. The integration root lane is one x86 container action declared by `tests/proof_execution.json`. Native threads stay at one. The command supplies no fleet shard count or CPU host tag.

## Evidence

Read `verdict.json` and each lane's `LANE.pbtest.json` in the printed directory. A pass requires a zero client exit, complete selected-file coverage, reconciled pytest outcomes, and content-verified CAS receipts and payloads. Record the counts, skips, wall time, action keys, receipt hashes, runtime generation, and the reviewed source commit. The command writes `INCONCLUSIVE` if no PB report exists. Missing or failed CAS verification fails the verdict.

`LANE.pbtest.json` retains each shard's output; `failed_shards` in `verdict.json` names the lane, action key and files of every failure. Read failing tracebacks and the actual resulting state; a terminal `done` status alone does not prove the asserted behavior. For GPU qualification, retain the `prismabuild-gpu-qualification` record with the device, CUDA version, iteration count, and equal actual and expected checksums. The GPU path allocates CUDA tensors and checks GPU matrix products against CPU results.

For a fix, demonstrate that its regression fails on the prior source and passes on the proposed source through PB. The optional `regression-test` CI check establishes structural test changes; the Principal Engineer judges whether those tests cover the fix.

## Cleanup

These commands start no local daemon. Wait for each submitted run to settle; PB owns its execution checkout and passing test scratch cleanup. Do not remove a shared queue record, CAS object, or another run's files. If interruption leaves work active, inspect the printed action keys and use the published withdrawal procedure for those exact keys.

Keep `$PB_EVIDENCE` and confirm that its `verdict.json` and lane reports still exist after the run. Evidence survives cleanup. Use a fresh directory for each rerun so prior output cannot become the new verdict.

For map maintenance, use `maintain-verification-skill` when it is available.
