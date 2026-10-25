# Run the test tiers

From a task checkout, run the tier that matches the change:

| Tier | One command | Scope |
| --- | --- | --- |
| Fast unit gate | `python3 tools/verify.py fast` | Curated CPU contracts, under ten minutes |
| CPU integration | `python3 tools/verify.py integration` | All remaining CPU test files |
| NVIDIA qualification | `python3 tools/verify.py gpu` | Real GPU allocation and checked CUDA matrix products |

The commands use the published PrismaBuild `pbtest.py`. PB selects shard membership, placement, and concurrency. Each CPU shard reserves one CPU and four GiB RAM. Each GPU shard reserves one CPU, eight GiB host RAM and two GiB GPU memory. Native threads are one per worker, and validation runs at priority -10.

The default worker Python paths are `/home/rob/venvs/pb-cpu/bin/python` and `/home/rob/venvs/pq-cu130/bin/python`. Pass `--python /absolute/worker/python` for another qualified environment. No coordinator installation is needed.

Run `python3 tools/verify.py TIER --plan` to inspect its file population and command without execution. Fast and GPU membership lives in `tools/verify.py`; integration is the complement. New CPU tests therefore enter a tier automatically. Removing a declared fast test refuses the gate instead of silently reducing coverage.

Each invocation prints an evidence directory outside the checkout. Pass `--report-dir /absolute/fresh/directory` to choose one. Read `verdict.json` for counts, skips, wall time, action keys, content-verified CAS receipts, and the verdict. `pbtest.json` retains the full shard output and reconciled pytest outcomes. Evidence never enters the sealed source tree. A reused evidence directory refuses.

A fast pass needs complete coverage and a wall time below 600 seconds, including queue, submission, and receipt verification. Distinguish a budget failure from an assertion failure. The per-test alarm is 60 seconds for fast tests and 300 seconds for integration and GPU tests. Per-shard payload deadlines are 540 and 1800 seconds respectively; they do not impose a whole-run kill or bypass cleanup.

GPU qualification requires an actual NVIDIA CUDA PyTorch environment. Missing tooling, missing device access, and skipped GPU checks fail qualification. This fixed-work correctness gate does not establish performance or campaign acceptance.

Use the repository [verification skill](../.agents/skills/verify-prismabuild/SKILL.md) for doctor, evidence, and cleanup. The [regression-test rollout](testing_regression_gate.md) describes the optional PR check and its five-PR promotion gate.
