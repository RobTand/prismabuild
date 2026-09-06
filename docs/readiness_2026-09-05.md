# Fleet readiness qualification — 2026-09-05

PrismaBuild's live execution plane is the shared CAS and pull queue on sparky,
sparklina and dl380g10. This qualification completes the open lane-sweeper PR
and integrates the existing issue work, repairs fleet availability, and makes
PrismaBuild mandatory for agent and subagent tests and GPU execution. Native
SLURM cutover and a Dagster service are separate deployment stages.

## Qualified baseline

Source `d157067f4603150db2a2c41b994b5fcc363a908d` was published as
`d157067f4603-1788648527-98e68f4fd774`. Its 208 published file digests were
verified; the publication receipt SHA-256 is
`3a3fbdc51d5e3982bfc4c3b8a3f3ad2494434a60e2a49a0ac266aa7d2e226a47`.
All three hosts adopted it. Subsequent CPU-tier qualification is recorded
separately below; these baseline counts describe this exact source.

| Host | Python | Test processes / native threads each | Result |
|---|---|---|---|
| dl380g10 | 3.14.4 | 72 / 1 | 1,835 passed, 3 skipped; 13.42 s pytest time |
| sparky | 3.12 | 18 / 1 | 1,835 passed, 3 skipped; 11.93 s pytest time |
| sparklina | 3.12 | 18 / 1 | 1,835 passed, 3 skipped; 11.34 s pytest time |

The three skips on each host were the optional Dagster dependency and the two
opt-in SLURM smoke harnesses. The latter ran separately through PB: the
single-node harness passed **23/23** rows against source `a3aca6aa5010`, and
multinode passed **12/12** against `040deaefb67f`, including node failure and
controller restart. These are privileged isolated container controllers, not
installed host services. Expected topology/verification refusals in those
containers are documented in their smoke logs.

The published `pbtest` path also passed **10/10 shards**, with seven pytest
workers per shard and one native thread per worker (70 CPU reservations in
aggregate). Both GB10 hosts completed an admitted CUDA matrix multiplication
using PyTorch `2.13.0+cu130`: a 512-by-512 matrix of ones squared produced
all 512s, sum `134217728.0`, and a successful CUDA synchronization. These
checks establish execution and correctness for that workload, not a GPU
performance or saturation result.

CPU actions, in table order:

- `b23bb4851982c8058bc555c4f51e08ad8b577f1474f1d451a8be314d9eba66e3`
- `5ff7256fed1e72a9857872e0fde3265d2c052b4c4da349dbb1e111272dd025a0`
- `a5895b5ed5954fadd7f6cc7051ce7207c08ba95ad023f0cac13a6897c42dcb13`

GPU actions, sparky then sparklina:

- `57ccff88f5497f2a47ec43bd6802795993434a0623bbf6cec31fa8d65e6acb81`
- `b406a793a82076fde72cb0afd110b10fe08258a2179db925755b9253c89d160a`

SLURM single-node and multinode actions:

- `37b91c93db5d6a3802dc2ccbe2ad87e463970c00fe72f8e8f5fa311b83c12eb5`
- `21dd4decb902127f65300e2ce1a11a74c2b405267a8ab2ba8406af29aff2f472`

CAS receipts live under
`/mnt/shared/prismabuild-fleet/cas/actions/v3/<first-two-key-characters>/<key>.json`.
Terminal records and payload hashes/sizes were checked independently of the
submission wrapper. On sparky, `/home/rob/pb-logs/primetime/final-receipts.json`
records keys, result hashes and summaries; `final-*-submission.json`,
`final-*-output.log`, `final-fanout.json` and the `slurm-*-submit.log` files
retain invocation and outcome evidence. Host Netdata samples are retained
alongside them; CPU charts include I/O wait and are not a compute-throughput
measurement. No before/after performance improvement is claimed.

## Repairs and negative results

The old Sparklina hostname no longer matched its supervisor shape. Explicit,
unique alias resolution restores its workers while retaining both placement
tags. dl380g10 had no effective shared path at `/mnt/shared`; a persistent bind
mount now maps its server-local `/storage_pool/shared` there after ZFS mounts.
The client exports now use `sync`, with `root_squash` retained and ZFS
`sync=standard`. This is verified configuration, not power-loss qualification.
Backups and exact changes are recorded in
`/home/rob/pb-logs/fleet-readiness/infrastructure-2026-09-05.md`.

Real x86 Python 3.14/Btrfs execution and ARM NFS execution exposed cleanup
failures that local tests had missed: directory descriptors could retain an
exhausted iteration position, and unlinking an open NFS owner/payload left
`.nfs` files behind. Fresh inode-relative directory descriptions and closing
owned descriptors before unlink/removal fixed the observed failures. The
reaper needed a writable descriptor for exclusive NFS locking. Focused
qualification passed 180 core cases on x86, 11 ingest cases on NFS and 25
reaper cases on NFS. Disposable qualification namespaces were outside the
production CAS. No live-store garbage collection was performed.

Earlier failed full-suite attempts also exposed concurrent source-tree stamp
writes, a timing assertion dependent on worker load, and stale fault-injection
fixtures. Stamps now enter a private snapshot index; timing tests observe the
actual poll seam; fixtures exercise the updated contract. Failed attempts and
the initial GPU image entrypoint failure remain in the artifact directory.
They are superseded evidence, not counted as successful qualification.

## Issue and work-in-progress disposition

PR #169 supplies unattended lane reconciliation. Its review fixes prevent
false swept results, preserve withdrawal diagnostics, isolate malformed input,
and serialize terminal generation comparisons across hosts with permanent
POSIX lock files. Bidirectional lock exclusion was exercised between the NFS
client and server-local ZFS path.

The integration resolves the following issue groups:

| Issues | Resolution |
|---|---|
| #115, #126 | Explicit immutable wrapper staging; private closure-stamp overlay, with no new submitting-tree litter |
| #116, #118, #122 | Ownership-aware staging and conservative quiescent-store reaping |
| #117, #120, #132 | Canonical Git identity independent of personal configuration; snapshot proof for non-pbrun definitions |
| #123, #125, #128, #130, #135, #139, #144 | Unambiguous exit vocabulary, truthful record errors, refreshed terminal reads and lane self-healing |
| #131, #136, #140, #145, #154, #163 | Claimed-action drain, current withdrawal details, bounded reset diagnostics and real admission acknowledgement |
| #141, #143, #148, #156, #157, #159, #160, #164, #166, #181 | Behavioral test coverage replaces source greps, copied implementations and vacuous assertions |
| #170, #172, #173, #174, #176, #177, #178, #179, #182, #183, #184 | Deterministic synchronization, mandatory shellcheck, hermetic fixtures, actual atomic publication checks and cwd-independent Git |
| #124, #127 | Already implemented in merged PR #112; memory default and key-carry cleanup remain part of native installation qualification |
| #147 | Keep manifest re-run recovery: sweeps file endings, while manifest recovery can resubmit unfinished work and supports the pool |

Audit #57 is dispositioned by these fixes and the following retained design
choices. Bundle bytes remain a sealed transport input with pinned Git packing;
changing key derivation again is not required for this release. Sparse checkouts with absent tracked paths now refuse, protecting the sealed
working-tree proof. Operator tools validate shard ranges and manifests, report
partial/error exits, and distinguish an absent queue; their help and operating
guide have been updated. Historical malformed render artifacts,
quarantines and original worktrees remain retained evidence; they are not
current result authority. Their sizes, hashes and disposition are recorded in
`/mnt/shared/prismabuild-fleet/historical-artifact-disposition-2026-09-05.json`.
NFS cache visibility can still cause redundant work;
immutable first-writer publication decides the result. The earlier SLURM adapter
and its optional Dagster integration remain until the documented Phase 3.
This baseline alone did not certify native cgroup/GPU containment. Subsequent
containment evidence is recorded below; power-loss recovery and a complete
observability-service rollout remain outside these live proofs.

## Mandatory agent routing

[The execution policy](agent_execution_policy.md) applies to every agent and
subagent, across projects. It is installed and read back on all three hosts in
Codex, Claude, Gemini and Pi global instruction files and `/home/rob/AGENTS.md`.
Claude's Bash pre-tool hook invokes the published `require_pool.py`; the
activation flag is present on every host. Existing configuration was preserved
and backed up. Installation/readback receipts are `policy-*.json` and
`policy-verified-*.json` under the qualification artifact directory.

This is a working policy plus a command guard, not an OS security boundary.
Already-admitted children execute directly. Further tests and GPU work use
published PB entrypoints, declare combined CPU/memory/GPU demand, and verify
terminal records and CAS output. New workers must receive the same policy.

## Adaptive admission, containment and unattended clients

The adaptive controller admits cheap CPU work beyond declared physical capacity
when fresh host and per-action telemetry supports lending. On sparky, a declared
20-CPU donor ran alongside three one-CPU borrowers. During the donor's busy
phase, measured host activity reached 20/20 CPUs and CPU pressure reached
0.99993; a new sentinel stayed queued and ran after pressure subsided. Existing
cheap actions survived. Borrowing an idle preferred CPU preceded using a free
fallback CPU. Measurement actions retained exclusive placement. The observer
recorded 302 host samples and Netdata snapshots; this is an admission and
isolation result, not a throughput speedup claim. Evidence is
`adaptive-qualification-report.md` and `adaptive-live-sparky-4a5da96e4ddb/` in
`/home/rob/pb-logs/primetime`.

The supervisor grew from five to six workers when six admitted waiters could
run, and shrank to five after completion. All six successful CAS outputs,
leases, reservations and kernel-scope removals were checked. Across all three
hosts, adopting a new supervisor generation preserved PID, process start
identity, the held file lock and active worker leases. Evidence is
`elastic-proof-1788655679/manifest.json` and `supervisor-reexec-*.jsonl`.

Each admitted action receives an owned cgroup with its declared hard memory
budget and zero swap allowance. The earlier 90% soft watermark caused heavy
reclaim before the hard limit; it was removed after live measurement. A direct
96-MiB scope and a Docker-backed 96-MiB scope reached exactly that peak and
killed only their greedy work. The Docker case verified cleanup of both owned
containers, while a neighboring healthy scope survived. Action
`312e7d2bcf3b6fddb88f8cc2c9cda4125cc293c575740580f8f9763292c59cdc`
holds the checked result.

The running GPU monitor also stopped a process whose observed GPU allocation
exceeded its one-GiB scope budget after two confirmed samples. A separate
128-MiB tensor job kept making progress afterward. Action
`f2444b3bb856e02b1baf62a09ae4c89e0f17f069e82e2aba8585c97693b30403`
holds the checked result. GPU monitoring is sampled and uses a conservative
per-process lower bound to avoid double-counting shared memory. Aggregate
excess across several smaller GPU processes can escape that bound. Predictive
host-memory protection has deterministic test coverage; these live proofs do
not certify every impending host OOM or instantaneous GPU allocation burst.

All three hosts have the automatic client-upgrade timer. Publication occurred
while one witness action per host was active: brokers retained their original
process identity while draining and every witness completed successfully.
Timers then installed the new clients 23–27 seconds after release. Installed
file hashes matched the manifest, timers were active, and transaction journals
and witness cgroups were absent. Root-squashed NFS remained enabled. Evidence
is `/mnt/shared/prismabuild-fleet/qualification/client-upgrade-20260905/summary.json`.

The shared PrismaBuild skill is installed for Codex and Claude on all three
hosts; global instructions also guide Gemini and Pi. Skill validation through
PB passed in action
`aad50963e7ce660639222874f1b83bfed364202dba631a589d51602a6e8cde62`.
Rob explicitly selected guidance and the existing command guard for routing:
no GPU device permissions, user identities or Docker access were tightened.

Sparky's one-second Docker/containerd pulses came from Netdata's Docker
inventory collector. Changing that collector alone from one to ten seconds
reduced measured combined daemon consumption from 0.9192 to 0.076 CPU cores
in paired 25-second observations, freeing 0.8432 cores (91.7% reduction).
Docker and containerd were not restarted. The profiler and observations are
retained under `docker-pulses/` in the qualification artifact directory.

## Final integration recovery checks

A full-suite run exposed lazy directory iteration on Python 3.12 and a marker
helper whose new diagnostic argument broke its existing caller. Both were
corrected; 15 focused ARM tests passed. A real OOM then exposed asynchronous
scope cleanup being delayed until lease expiry. PB now retains the original
finish status and detail, retries on the next local poll, and releases capacity
only after cleanup is confirmed. The 272 pool tests and six focused cleanup
regressions passed.

Final authority review found that a lost create response could leave a broker
scope without durable worker ownership. Claims and leases now persist the
exact creation intent before the RPC. Recovery retrieves that authority or
durably cancels an absent attempt, fencing late create requests. Older brokers
refuse the tagged protocol before creating anything, so new workers defer
without consuming an attempt while clients converge. Combined recovery tests
passed 163 cases through PB; the red and green receipts remain in the artifact
directory.

The nested SLURM smoke originally assumed CPU zero and used nproc under a
one-thread OpenMP environment. It now uses slurmd's actual hardware topology
and SlurmdSpecOverride to exclude parent-unavailable CPUs. It retains PB's
cpuset and SLURM's affinity/cgroup plugins. The corrected multinode harness
passed 12/12 contracts, including node failure and controller restart, in action
`8dee0c281b35ce6720090149e4804cabd88ea6e2c0d9ea42f56b8ff60575760a`.

The single-node smoke then exposed incorrect hierarchical OOM attribution: a
one-GiB nested job hit its own limit while its eight-GiB enclosing PB action
peaked at 1,185,460,224 bytes. The parent local OOM counters were zero. PB now
uses the parent-local OOM counter to identify exhaustion of its own budget;
hierarchical victim counts remain diagnostic. Regressions cover child-only
OOM, parent exhaustion with descendant victims, and exhaustion before a victim
is killed. The withdrawn negative run retains an empty frozen cgroup tombstone
for an ambiguous Docker RPC; its exact container census is empty. This retained
safety record is not active work.

## Qualified CPU admission and containment baseline

Production source `55178b0c8f1de9d0f625a25d7d6f4e31b55b3d51` was published as
`55178b0c8f1d-1788657440-9e867203c29a`. All 244 file hashes matched; publication
receipt SHA-256 is `a55e10ede4dacb3215d4a9035dd44ec590a53112dda45e40f20b022bbe778cb1`.
All three worker fleets and installed privileged clients converged. Installed
file hashes, live updater status, active timers and absent transaction journals
were checked in `client-adoption-55178b0c8f1d-1788657440-9e867203c29a.json`.

| Host | Test processes / native threads each | Full-suite result | Action |
|---|---|---|---|
| dl380g10 | 72 / 1 | 2,179 passed, 3 skipped; 15.82 s | `52296b88bf9977639acc342dc570490694f90587bdc404995ce89b9cd8197c3c` |
| sparky | 18 / 1 | 2,179 passed, 3 skipped; 15.01 s | `a3e002c52f4d1ba2d5103edba06df425e6c6dd037f062722291d292bb393c7df` |
| sparklina | 18 / 1 | 2,179 passed, 3 skipped; 12.13 s | `fdad5dfde4bbd548346413a91180c25771d5a9801c55d08b5aefd6135c9e64ac` |

Receipts, their canonical digests, payload sizes/hashes and executed terminal
records were independently checked. The three skips remain optional Dagster
and the separately executed container smoke harnesses. Resource telemetry is
retained in `primetime-qualified-resource-use.json`; allocated processes do not
imply continuous full CPU occupancy. The separate host sampler started after
the suites had finished and is not evidence of their peak utilization.

The `pbtest` fanout passed **10/10 shards, 2,179 tests and 3 skips** on x86 with
seven test workers and one native thread per worker. Its first run exposed a
100-ms rendezvous test deadline expiring during manifest validation under load.
The test now advances its clock at the arrival wait, preserving the exact
refusal and no-task-execution assertions. The 169 core tests passed, followed
by the complete fanout. This change is test-only (`ffb260f`); production code
remains the qualified baseline above. All ten CAS results are indexed in
`adaptive-final-fanout-receipts.json`; the failed run is retained separately.

The final live OOM action
`9892987ebf66679be9a6924adb9f1929bcfc8047edcbf7d7c34901a7f96d7a4c`
filed a normal failed-137 outcome with `memory_limit_oom`, local OOM count one,
five killed descendants and a one-GiB peak at its one-GiB limit. Its healthy
companion `21f444e17b23b66ac1b59d05c3ba25db547f1eeedb502e765a8e4bf969ed2dfa`
kept the same PID after that ending and completed successfully. Both owned
scopes, payload PIDs, claims and reservations were absent afterward. An
unrelated admitted job invalidated the harness's global-idle assertion; the
independent exact-owned verification passed and preserved that unrelated work.

The aggregate direct/Docker probe
`b287219b25f034cbd327ea2125042d207752003ed94e0ec57a0f02302290625b`
passed again with parent-local OOM attribution. Both 96-MiB greedy groups were
stopped, their healthy witnesses survived, and all four scopes and their exact
Docker inventories were empty/absent. The final single-node smoke
`f497582321b282483835196f4828f662168276504b88fe460bb09f372b065ab9`
passed **23/23** rows. Its intentionally OOM-killed child left the enclosing
job healthy: hierarchical kill count one, parent local OOM zero, peak
1,184,915,456 bytes below its eight-GiB cap. The subsequent healthy job and
singleton checks passed. Its CAS output and final container/scope cleanup were
verified. The multinode **12/12** result is recorded above.

Both GB10 hosts also repeated actual CUDA correctness through this broker
version: actions `dc833860b658dd8c8cddb36ae43cd9806dabcf11da67127e366ad84c3ea948a9`
and `e2a514811f815bc045999fa655fbe53e70d56c2d510df2a439f4c1d7280154ad`.
Each produced the expected matrix values and sum, with actual Docker affinity
matching admission. These are correctness checks, not saturation benchmarks.

This baseline qualifies adaptive CPU admission, containment, client upgrades,
and agent routing. GPU admission still uses historical concurrency slots in
this version. Rob has prioritized replacing those with adaptive GPU admission
and separate physical VRAM/system-RAM accounting; that follow-up is not
certified by these baseline results.
