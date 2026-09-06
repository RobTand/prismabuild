# Adaptive GPU fleet qualification — 2026-09-05

The GPU candidate is source `d8f82f3a48f6eddf21020b8a1ff2d7d25d46c218`,
published as `d8f82f3a48f6-1788660636-47bb2f84514a`. Its 250 published file
hashes were independently verified. The CPU admission, OOM containment and
agent-routing baseline remains recorded in [readiness_2026-09-05.md](readiness_2026-09-05.md).
The admission contract is in [design.md](design.md#adaptive-gpu-admission-and-independent-memory-domains).

## Deployment and integrated validation

The fleet first adopted updater bridge `065bd8ef8362a71fc700140598931add79249544`
with the previous broker bytes unchanged. Each installed updater hash was
verified before publishing the GPU dependency. All three workers then adopted
the candidate automatically: five installed member hashes and four loaded
broker-module hashes matched the publication; brokers were healthy, timers
active, and transaction journals absent. The public snapshots were root-owned,
mode 0644, and less than one second old when checked. Both GB10s reported one
physical GPU and `shared_system`; framebuffer totals were correctly absent.
The x86 host had no NVIDIA device, an incomplete GPU snapshot and healthy CPU
admission. No active work was cancelled for this upgrade.

Artifacts are under `/mnt/shared/prismabuild-fleet/qualification/` in
`client-upgrade-bridge-20260905/` and `client-upgrade-gpu-candidate-20260905/`.
Independent checks are also retained under `/home/rob/pb-logs/primetime/` as
`client-adoption-065bd8ef8362-1788660094-615bd1f26a0b.json` and
`client-adoption-d8f82f3a48f6-1788660636-47bb2f84514a.json`.

All tests ran through the published PrismaBuild client. Final concurrent fleet
suites used one native thread per pytest worker:

| Host | Pytest workers / declared RAM | Result | Action |
|---|---|---|---|
| dl380g10 | 72 / 40 GiB | 2,404 passed, 3 skipped; 17.06 s | `40b295c647c3cdbf416e6da00d8bd271a20da7a1a68cb150236e751ae9aaec50` |
| sparky | 18 / 24 GiB | 2,404 passed, 3 skipped; 16.13 s | `ce6d0f5e824828b2e4e556464feb135d832115763a5342aea2d75721162b7c37` |
| sparklina | 18 / 24 GiB | 2,404 passed, 3 skipped; 12.29 s | `4e895f2be04ee151f8a611162d552c7d0c7c66ba95812070df386a5bd809478b` |

The skips are optional Dagster and the two opt-in SLURM container harnesses,
which were independently executed for the preceding containment baseline.
These GPU changes do not claim a native SLURM deployment. Terminal status,
canonical CAS receipt digests, and actual result hashes and byte counts were
checked, not just submission acknowledgements. Results are indexed by
`gpu-candidate-d8f82f3-receipts.json` in the primetime artifact directory.

Integration reproduced and corrected an order-dependent module mock, the real
GB10 N/A framebuffer refusal, and unrepresentable GPU budget conversion.
Focused regressions cover independent discrete VRAM reservation, missing/stale
telemetry, canonical memory domains, exact attempt attribution, exclusivity,
probe response/plateau persistence, and recovery. Details and earlier red runs
are in [adaptive_gpu_admission_2026-09-05.md](adaptive_gpu_admission_2026-09-05.md).
The agent skill passed its validator through PB, action
`3ce4f6d252ff99c3876132e3f5c3aaf56dc67fa8fa9942fbb245f63494cc0b03`.

## Paired useful-work measurement

Both campaigns used Sparklina and pinned image
`sha256:9f9b9f05b17531399ba66dc6415b054cf5d68c82270626d0e9150e75c808435f`.
Six independent actions each declared one CPU, one GPU and 8 GiB RAM, with one
native thread, and computed 3,000 batches of 128 embeddings. The synthetic
pipeline performs real CPU feature preparation and a four-layer 4,096-wide
BF16 GPU encoder. Workload source, batch shape and parameters were unchanged;
all 18,000 per-iteration output checksums matched exactly. Each action retained
CPU/CUDA profiler traces, CUDA event timings and scoped memory evidence.

The baseline ran source `55178b0c8f1de9d0f625a25d7d6f4e31b55b3d51` with the
historical three-job GPU cap. The candidate admitted all six simultaneously on
one physical GPU, without discounting any RAM or GPU memory reservation.

| Metric | Baseline | Candidate |
|---|---:|---:|
| Completed embeddings | 2,304,000 | 2,304,000 |
| First useful work to last useful output | 129.525 s | 81.934 s |
| Aggregate useful throughput | 17,788 embeddings/s | 28,120 embeddings/s |
| Submission to verified receipts | 145.254 s | 95.194 s |
| Sampled GPU energy during useful-work span | 2,115.37 J | 1,407.79 J |
| GPU energy sample coverage | 100% | 100% |
| Embeddings per sampled GPU joule | 1,089.17 | 1,636.61 |
| Maximum simultaneous owned GPU claims | 3 | 6 |

This one paired campaign measured **58.08% greater aggregate throughput** and
**33.45% less GPU-reported energy**. It measures the combined candidate,
including faster nonempty-queue polling, not an isolated controller-only effect.
GPU-reported joules exclude CPU and whole-host/SoC energy. The result is specific
to this workload and environment, not a universal application speedup. GPU
utilization percentage was not used as a saturation diagnostic. Host CPU, RAM,
PSI, Netdata and GPU power/clock/residency observations are retained.

Evidence is under `/home/rob/pb-logs/gpu-adaptive/`:
`before-pipeline-six-sparklina-dc69be4e0c/`,
`after-pipeline-six-sparklina-5bb8f79e37/`,
`pipeline-before-after-comparison.json`, and
`pipeline-root-independent-verification.json`. The latter verifies all twelve
actual CAS payloads and the 18,000 exact checksum matches. CUDA contexts first
appeared 2.45–3.16 seconds after admission; no startup plateau blocked this run.

## Independent GPU budget containment

PB action `ecc182aeaedd6eebf4d44f74d60b594efd7df08f180a635181ea537ad4601a0d`
ran on Sparky through the candidate, CPU 2, host RAM 16 GiB, exclusive GPU and
an explicit aggregate 2 GiB GPU budget. Two owned child scopes each had a
4 GiB system-memory cap and a separate 1 GiB GPU cap. The offender allocated
1,536 MiB on CUDA; its healthy neighbor allocated 128 MiB and kept progressing.

The daemon observed 1,825,570,816 GPU bytes for the offender: above its
1,073,741,824-byte GPU budget but below its 4,294,967,296-byte shared physical
memory allowance. After two attributed samples it stopped only that scope
with `gpu_memory_budget_exceeded`, returning 137. Healthy progress advanced
from 9 to 14. Both inner scopes, the outer action scope and owned processes
were absent afterward. The action returned zero only after cleanup.

Its actual CAS receipt digest is
`8fca95fc235e0fad3f47440ac102b58617037d9dfc9f90a62883817eef305fcd`.
The verified verdict, payload and cleanup are retained as
`explicit-gpu-cap-{verdict,receipt,cleanup,root-verification}.json` and
`explicit-gpu-cap-output.log` under the primetime artifact directory.
The preceding harness attempt observed the correct stop but incorrectly
expected budget fields in a status response; that negative result is retained
and was not counted as a passing qualification.

## Hardware scope

Both current live GPU hosts are GB10 shared-memory machines. Discrete VRAM
admission and separation from system RAM have modeled regression coverage;
no desktop discrete GPU was available for live qualification. Adding one still
requires real-device telemetry, admission and containment checks. Unknown
hardware or memory counters cannot be treated as free capacity. Multi-device
placement remains unsupported by the current adaptive controller.

## Queue fanout and agent guidance

The final published `pbtest.py` fanout completed all ten shards on dl380g10,
seven pytest workers and one native thread each, 4 GiB per shard. It passed
2,404 tests with the same three optional skips. All ten normal terminal
records and actual CAS payloads were independently checked; the index is
`/home/rob/pb-logs/primetime/gpu-candidate-fanout-receipts.json`.

Codex and Claude skill links on all three hosts resolve to the published
`skills/prismabuild` directory. Its operating instructions now describe
adaptive admission and separate GPU memory budgets. Standing global guidance
routes agent and subagent tests/GPU work through PB. No GPU user, device or
Docker permission lockdown was added; this follows Rob's preference for
clear instructions and gentle guidance.

## Saturation backoff and identical-host behavior

The stable compute proof on Sparklina first observed five fresh attributed
samples at 79.68–80.99 W and a stable 2,372 MHz clock. After one controlled
concurrency probe, the controller measured a 0.193 W response against a
2.448 W noise/deadband margin and latched a plateau. A fresh third GPU action
remained queued for an explicitly observed 5.597 seconds while both compute
holders continued, then later ran and completed successfully without manual
admission-state edits or terminating either holder.

The first holder finished at Unix 1788661236.4994893; the second finished at
1788661261.3275497. The waiting action was claimed at 1788661261.4965017, after
both had naturally completed. All three normal terminal records and actual CAS
outputs were independently verified. Evidence lives in
`/home/rob/pb-logs/gpu-adaptive/after-compute-gate-stable-sparklina-c1aa8e2fcb/`.
The earlier gate attempt used a fixed eight-second warmup and sampled rising
power/clocks, so the subsequent measured response correctly allowed another
probe. That run's workload results were valid but its intended stable-plateau
precondition was not met; the negative evidence remains retained.

Sparky separately admitted three concurrent ordinary GPU actions on its one
physical GB10, with one physical reservation and two sharing reservations.
All used `shared_system`, preserved 8 GiB budgets and completed normally.
This demonstrates the same live sharing contract on both machines, not a
Sparky before/after performance claim. Exact results are retained in
`after-shared-three-sparky-db99e2d3aa/` under the same artifact root.
`gate-and-sparky-root-independent-verification.json` checks these six actual
CAS payloads and terminal records.

All 24 retained observations during the 24.828-second interval between compute
holder exits preserved the plateau, with the remaining GPU context and
81.02–86.84 W device readings. The next action was claimed 0.169 seconds after
the last holder's terminal. `plateau-exit-persistence.json` records this live
undersubscription interval. Recovery after all holders clear is live-proven;
same-holder recovery after sustained power reduction has modeled regression
coverage, not a separate live qualification claim.
