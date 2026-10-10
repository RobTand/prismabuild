## Problem

A CPU-only measurement row on dl380g10 holds the **whole 80-thread host**. While it runs, `adaptive_cpu.Controller.decision` refuses every other row that is not its own dependent with `measurement_holder`, however few CPUs the measurement declared.

**Live case, 2026-10-05.** Testcost's timing capture `5a4dcf687cb4` declared **4 CPUs / 9 GB** (tags `dl380g10, x86, interpreter-path-v1`). It was claimed 14:57:14Z. The denial records name it as the holder (`isolated_by` = itself) for **18 distinct rows (373 denial passes)** over 15.6 minutes, merge suites included. A sample at 15:12Z showed the host ledger with **39 of 79 CPU tokens unheld**. The CEO withdrew the holder at 15:11:52Z ("measurement holder blocks merge suites"), after which all 18 rows were claimed; so the cost was paid twice: the merge suites waited, and the 14-minute timing capture was thrown away. (I did not capture the blocked rows' declared CPU before they were claimed, so I do not claim all 18 would have fit in the 39 free tokens.)

## Why it is host-wide today (read from `origin/main` 6781c549; cites are to `src/prismabuild/adaptive_cpu.py`)

- `decision()` ~1475-1490: with a measurement holder present, a non-dependent row returns `refuse("measurement_holder", holder=..., isolated_by=...)` unconditionally. It never asks which CPUs the holder holds.
- The same file already has the per-CPU machinery this needs: `_held_cpus(holders)` (973), `foreign_per_cpu_busy` and `_measurement_foreign_ambient` (1015, `PER_CPU_FOREIGN_MAX = 0.10`), `_predicted_cpus(need, eligible_cpus=...)` (1126) and the ledger's `free_cpu_tokens(..., eligible_cpus=)` (#1210), which already lets a decision restrict which CPU tokens a claim may take.
- The measurement's own admission is judged on `IDLE_FIELDS = ('busy_cpus', 'psi_some')` host-wide (`_measurement_idle_refusal`, 956). #1422 (closed) let a *clear per-CPU attribution on the measurement's own CPUs* bypass the aggregate `busy_cpus` half, and deliberately **preserved** `measurement_holder` isolation. This issue is the other direction: what a *running* measurement forbids.
- Not yet read by me, listed so the author audits them: `pool.py` `_measurement_holder_tail_refusal` (616), the early-admission use at ~7481, and the claim-path withhold that reads `decision.get("reason") == "measurement_holder"` at ~21421.

## What the isolation is protecting (so the guard is derived, not guessed)

A timing is only comparable to the host's idle baseline if nothing else perturbs the measured CPUs. On dl380g10 (verified with `lscpu` and sysfs): Xeon Gold 6230, **2 sockets x 20 cores x 2 threads, 4 NUMA nodes of 10 cores / 20 threads** (node0 = CPUs 0-9,40-49; node1 = 10-19,50-59; node2 = 20-29,60-69; node3 = 30-39,70-79); SMT siblings are CPU N and N+40 (`thread_siblings_list` of cpu0 = `0,40`). The ledger already models the pairing: `cpu_tiers.preferred` = 0..39, `fallback` = 40..79. Interference classes, nearest first: (1) the SMT sibling of a measured CPU (shares the core), (2) others in the same NUMA node / L3 / memory controller, (3) the other socket (memory and power budget), (4) host-wide I/O, and dl380g10 is also the file server: knfsd and ZFS threads run anywhere and cannot be partitioned away by admission.

So "a CPU measurement costs only its cores" is too strong. The honest target is: a measurement costs **its CPUs plus a guard band whose size is measured**, not the whole host.

## Design

**Phase 0, calibrate (no code).** Pick the guard by experiment, per Rob's no-heuristics rule (cf. #997). Run a fixed CPU-bound and a fixed memory-bound kernel (the testcost timing workload itself is the right probe) 30x on 4 CPUs under five neighbor loads: idle host; load on the SMT siblings only; load elsewhere in the same NUMA node; load on the other node of the same socket; load on the other socket. Choose the **smallest scope whose timing distribution is within the existing idle-baseline tolerance** (the `idle_statistics` max/window already used by `_judge_idle`). Output: a table, and the chosen default scope. The scope options and what each costs a 4-CPU measurement, out of 80 threads:

| scope | reserved | threads left to other rows |
|---|---|---|
| host (today) | everything | 0 |
| socket | 40 | 40 |
| NUMA node | 20 | 60 |
| core + sibling | 8 | 72 |

**Phase 1, admission only, no cgroup change.** In `decision()`, when the only holders that are measurements are CPU-only x86 rows with an *enforceable declared CPU set*, replace the unconditional `measurement_holder` refusal by: compute the holder's **guard set** from its allocation (`_held_cpus`) expanded by the calibrated scope using the real sysfs topology, then admit the candidate **only onto CPU tokens outside the guard set** (pass the complement as `eligible_cpus` to `_predicted_cpus`/`free_cpu_tokens`, the hook #1210 added). A candidate that cannot fit outside the guard keeps today's behavior (`measurement_holder`, and the existing withhold/drain), so a wide row still drains the host. Rows the measurement spawns itself (dependents, #982) are unchanged. `--all-cores`, unbounded-CPU (`unbounded_cpu_not_exclusive`) and any row with no CPU declaration keep **whole-host** isolation, as does any GPU measurement on a Spark (GB10 hosts are one node and have no merge-suite contention). This needs no cgroup work: pool rows are pinned by `taskset --cpu-list <tokens>` (visible in the claim's argv) and children inherit the affinity.

**Phase 1b, the measurement's own start.** Admit it on a *scoped* idle verdict: per-CPU foreign busy on its guard set (existing `foreign_per_cpu_busy`, `PER_CPU_FOREIGN_MAX`), and PSI from the measurement's **own** scope slice rather than host-wide PSI. Verified on dl380g10: action slices `/prismabuild.slice/prismabuild-job<id>.slice` carry the `cpu` controller and a readable `cpu.pressure`, but **not `cpuset`** (`cpuset.cpus` absent in the slice, though the cgroup root offers the `cpuset` controller). Host-wide `psi_some` would otherwise be polluted by the very rows we now keep admitting, and the measurement could never start beside them (compare #569 for the closed host-wide-PSI complaint). This phase depends on #1422's per-CPU path and must not weaken it.

**Phase 2, hardening, optional.** Keep *foreign* (non-pool) processes off the guard set: put the non-measurement slices under a systemd `AllowedCPUs=` that excludes it (needs the cpuset controller enabled in `/prismabuild.slice`'s `cgroup.subtree_control`, a broker change). Not needed for Phase 1 and the main thing it buys is foreign load, which is already *detected* per CPU.

**Evidence instead of a new wall (D32).** During a scoped measurement, sample per-CPU foreign and held busy on the guard set and **stamp it into the row's receipt**; never refuse or re-run on it. A reader can then see how clean a given timing was.

## Risks and open questions

- **Timing validity is the whole point; Phase 0 can fail.** If even a SMT-sibling load shifts the timing beyond tolerance, the smallest valid scope is the socket or the host, and the gain shrinks to 2x or nothing. The issue should be closed as "not worth it" if so.
- **Fleet-file-server noise.** knfsd/ZFS and foreign agents (`hindsight-api` showed 211% CPU, a `git` 222% on dl380g10 at 15:12Z) run on any core. Phase 1 does not change that; the existing ambient check on the measurement's CPUs does.
- **Starvation.** A guard that grows with a measurement's CPUs can make the remaining capacity small; rows too wide for the remainder rely on the existing drain/withhold. Needs a test that a wide row is not starved forever.
- **Mixed generations.** The decision runs in whichever loop claims on dl380g10; during a rolling publication an old loop still refuses (the safe direction).
- **Topology source.** The ledger's `preferred`/`fallback` lists are not enough to find SMT siblings or NUMA nodes in general; read sysfs and make it part of the offer, or derive from `thread_siblings_list`. Unverified whether other x86 boxes in the fleet have the same layout (only dl380g10 is x86 today).
- Unverified: that `AllowedCPUs=` works under the broker's scope creation; the exact `pool.py` withhold behavior listed above.

## Acceptance

1. Phase 0 table and the chosen scope, recorded in the issue.
2. `Controller.decision` RED then GREEN: with a 4-CPU CPU-only measurement holding CPUs 0-3, a 4-CPU non-dependent row is admitted onto tokens outside the guard set and **never** onto a guard CPU; a row that fits only inside the guard still returns `measurement_holder`; a dependent row, `--all-cores`, an unbounded-CPU row and a GPU measurement keep whole-host behavior.
3. The measurement's own start no longer requires the whole host to be idle when its guard set is clear (per-CPU) and its own scope's pressure is below baseline; every existing refusal (`measurement_foreign_ambient`, stale or unknown sample, raw saturation, token, GPU gates) is preserved.
4. Live, after the next publication: during one real 4-CPU measurement on dl380g10, at least `80 - scope` threads keep admitting other rows, with the receipt stamped.
5. `docs/design.md` and the operating guide state the new rule and the calibrated scope.

Related: #982, #997, #1185, #1210, #1422 (closed), #569 (closed).
