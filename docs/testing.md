# Run the test tiers

From a task checkout, run the tier that matches the change:

| Tier | One command | Scope |
| --- | --- | --- |
| Fast unit gate | `python3 tools/verify.py fast` | Curated CPU contracts, under ten minutes |
| CPU integration | `python3 tools/verify.py integration` | Every other CPU test file, in three lanes |
| NVIDIA qualification | `python3 tools/verify.py gpu` | Real GPU allocation and checked CUDA matrix products |

The commands use the published PrismaBuild `pbtest.py`. PB selects shard membership, placement, and concurrency. Each CPU shard reserves one CPU and four GiB RAM, except the two-CPU lane below. Each GPU shard reserves one CPU, eight GiB host RAM and two GiB GPU memory. Native threads are one per worker, and validation runs at priority -10.

The default worker Python paths are `/home/rob/venvs/pb-cpu/bin/python` and `/home/rob/venvs/pq-cu130/bin/python`. Pass `--python /absolute/worker/python` for another qualified pbtest environment. The root lane uses its profile's exact interpreter. No coordinator installation is needed.

Run `python3 tools/verify.py TIER --plan` to inspect each lane's file population and command without execution. Fast, GPU and two-CPU membership lives in `tools/verify.py`. The root lane reads its files from `tests/proof_execution.json` and `tests/receipt_execution.json`. The one-CPU integration lane is the complement. New CPU tests therefore enter a tier automatically. Removing a declared test refuses the gate instead of silently reducing coverage, and so does declaring one file in two lanes.

Integration runs three lanes at once and passes only if all three pass:

- **One-CPU lane.** Ordinary CPU files through `pbtest.py`.
- **Two-CPU lane.** Files whose fixtures take two PB-assigned CPUs, through `pbtest.py --cpus-per-shard 2`.
- **Root lane.** The actual-root proof population of [proof test execution](proof_test_execution.md). These fixtures assert real UID/GID 0. One `pbrun` action uses the image and exact primary interpreter declared by `_proof_runtime_profiles/standard3144.json` on x86. `tests/run_root_population.py` stages the pinned inputs inside the action's private checkout, then runs `tests/run_proof_container.py --runtime-profile` over every root file. The native profile supplies the production installed-reader interpreter and its captured library closure. The legacy image runner lacks that interpreter. This tier provides test evidence; release qualification retains its separate collection index and acceptance gates.

Each invocation prints an evidence directory outside the checkout. Pass `--report-dir /absolute/fresh/directory` to choose one. Read `verdict.json` for counts, skips, wall time, action keys, content-verified CAS receipts, failed shards with their lane and files, and the verdict. Each pbtest lane's `LANE.pbtest.json` retains the full shard output and reconciled pytest outcomes, and `LANE.log` its client output. The root lane keeps `root.submission.json` and `root.endings.json`; read the action's log through the ending's paths. Evidence never enters the sealed source tree. A reused evidence directory refuses.

A fast pass needs complete coverage and a wall time below 600 seconds, including queue, submission, and receipt verification. Distinguish a budget failure from an assertion failure. The per-test alarm is 60 seconds for fast tests and 300 seconds for integration and GPU tests. Per-shard payload deadlines are 540 seconds for fast, 3600 seconds for integration lanes and 1800 seconds for GPU. A test that starts with less than its alarm left in the shard gets only the remaining time, so a shard that overruns reports its last tests at a bound shorter than 300 seconds. The deadlines do not impose a whole-run kill or bypass cleanup.

GPU qualification requires an actual NVIDIA CUDA PyTorch environment. Missing tooling, missing device access, and skipped GPU checks fail qualification. This fixed-work correctness gate does not establish performance or campaign acceptance.

In a source checkout, the verification skill `.agents/skills/verify-prismabuild/SKILL.md` covers doctor, evidence, and cleanup. Published runtime generations do not ship it, so it is named here rather than linked. The [regression-test rollout](testing_regression_gate.md) describes the optional PR check and promotion after five real fix PRs.
