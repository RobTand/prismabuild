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
Native cgroup/GPU containment, power-loss recovery and an observability-service
rollout are not silently certified by this qualification.

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
