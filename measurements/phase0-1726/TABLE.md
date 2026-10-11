# Phase 0 proxy evidence on dl380g10 for prismabuild#1726

## Verdict

The previous GO verdict is withdrawn. These proxy runs do not establish a
guard scope for the required testcost workload. Phase 0 remains incomplete.

Keep #1726 open. Do not release k2 to k5 from this prerequisite.
This is not an experimental no-go verdict, so it does not justify closure
of #1541 as "not worth it".

## Probe

The retained probe runs four SHA-256 and memory-copy workers on CPUs 0-3.
It does not run the testcost time workload.

`WORKER_SRC` in `phase0_main.py:35-46` defines the exact worker command.
`run_probe_once` at lines 114-138 starts one worker per probe CPU with
`taskset -c`. Each worker hashes a 1 MiB buffer 300 times, then copies a
64 MiB buffer 100 times. Both phase values use `time.perf_counter`.
The `cpu_s` field records hash elapsed time, not CPU time.

CPU 0 appears in every probe CPU set. However, `cpu0_busy` combines user,
system, and interrupt activity. It does not establish CPU 0 kernel load.
The retained rows contain no CPU 0 system-time or interrupt measure.
Action-wide system time does not identify CPU 0 or an individual run.

`LOAD_SRC` at line 48 hashes one 64 KiB buffer per neighbor.
The neighbor has no memory-stream phase. Thus, these runs do not establish
the guard required for memory-heavy neighbors.

The sealed commands are:

```bash
/home/rob/venvs/pb-cpu/bin/python phase0_main.py 1 15
/home/rob/venvs/pb-cpu/bin/python phase0_main.py 2 15
```

Each action declares 8 CPUs, 4 GiB memory, and an 1800 s deadline.
Both sealed requests have `task.task_class = "measurement"` and require
the `dl380g10` and `x86` tags. The receipt hosts are `dl380g10`.

CPU sets: probe always 0-3. Neighbors: idle none, smt 40-43, samenode 4-7,
othernode 10-13, othersocket 20-23. Round order is shuffled with a fixed
seed. Two actions (seeds 1 and 2) repeat the full set.

The stored case name `idle` means no added neighbor, not an idle host.
Its host busy mean is 14.7984 CPUs, with a maximum of 27.4398 CPUs.
The rows retain probe-CPU activity but no per-CPU neighbor activity.

## Topology (sysfs on dl380g10)

Xeon Gold 6230, 2 sockets, 20 cores per socket, 2 threads per core.
Node cpulists: node0 0-9,40-49; node1 10-19,50-59; node2 20-29,60-69;
node3 30-39,70-79. SMT siblings pair N with N+40 (cpu0 shows 0,40).
Package ids: cpu0 and cpu10 on 0, cpu20 on 1. Probe 0-3 sits on socket 0
node 0. Cases cover SMT sibling, same node, other node same socket, and
other socket.

## Tolerance

`src/prismabuild/adaptive_cpu.py:1281-1304` defines `_idle_statistics` and
`_judge_idle`. A sample exceeds when any requested field is strictly above
its reference maximum. `IDLE_WINDOW` is 256 at line 1255.
The default fields are `busy_cpus` and `psi_some`, not workload elapsed time.

`results.json.summary.tolerance_host` retains identity 80, window 256,
`busy_cpus` maximum 33.683642, and `psi_some` maximum 0.120927.
The captured baseline path is:

```text
/tmp/prismabuild-admission-1000/693d631920d476528729527e743dd4ec26856d873a4ad6d81ecea24c5116abcf.adaptive-cpu-v1/idle-baseline.json
```

The action stdout retains these baseline summaries:

| seed | samples | span s | busy mean | busy max | busy margin | busy stdev | PSI mean | PSI max | PSI margin | PSI stdev |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 61 | 304573.593 | 10.001633 | 33.683642 | 23.682009 | 5.578025 | 0.013406 | 0.120927 | 0.107521 | 0.020085 |
| 2 | 62 | 304803.818 | 10.031302 | 33.683642 | 23.652340 | 5.537708 | 0.013353 | 0.120927 | 0.107574 | 0.019926 |

No retained run exceeds either host maximum. This host-level check does
not distinguish a clean probe from an affected probe. The SMT timings
increase although all SMT host samples remain below these maxima.

## Proxy table with 30 runs per case

| case | wall mean s | wall max s | hash mean s | hash max s | copy mean s | copy max s | host over max | psi over max | cpu0 busy |
|---|---|---|---|---|---|---|---|---|---|
| idle | 2.5798 | 2.9805 | 0.8761 | 1.0948 | 1.3834 | 1.5371 | 0/30 | 0/30 | 0.9973 |
| smt 40-43 | 3.3621 | 3.5981 | 1.6287 | 1.6778 | 1.3667 | 1.6259 | 0/30 | 0/30 | 0.9990 |
| samenode 4-7 | 2.5908 | 2.9841 | 0.8822 | 1.3272 | 1.3815 | 1.5509 | 0/30 | 0/30 | 0.9981 |
| othernode 10-13 | 2.5758 | 2.7835 | 0.8826 | 1.0024 | 1.3755 | 1.4887 | 0/30 | 0/30 | 0.9971 |
| othersocket 20-23 | 2.5634 | 2.6390 | 0.8754 | 1.0079 | 1.3750 | 1.4570 | 0/30 | 0/30 | 0.9982 |

For diagnosis only, compare each run with the `idle` proxy maxima:
wall 2.9805 s, hash 1.0948 s, and copy 1.5371 s.
A hash or copy run exceeds when any of its four workers exceeds that maximum.
These comparisons are not an approved testcost tolerance or a guard verdict.

| stored case | wall over proxy max | hash over proxy max | copy over proxy max |
|---|---|---|---|
| idle | 0/30 | 0/30 | 0/30 |
| smt | 30/30 | 30/30 | 1/30 |
| samenode | 1/30 | 1/30 | 1/30 |
| othernode | 0/30 | 0/30 | 0/30 |
| othersocket | 0/30 | 0/30 | 0/30 |

The earlier report dismissed same-node maxima as noise without a rule.
This report makes no such classification and selects no scope.

## Receipts

| action | seed | result | returncode | host | elapsed |
|---|---|---|---|---|---|
| `b5c2710eed003e131529fce10f6a746a5e6d29c7d902e5ded900a98ad4250145` | 1 | executed | 0 | dl380g10 | 221 s |
| `f130852dc89e8cbf5c056af0af3152155e6fe55ed5b2a4d89ac3115466f66e19` | 2 | executed | 0 | dl380g10 | 223 s |

Done records live under `/mnt/shared/prismabuild-fleet/pb-queue/done/`.
Pilot topology action `154b6820058bba8be065b20d4175b313d2500008e828f98e125ec122eae457a9`
also passes on dl380g10.

These are two action receipts for 150 proxy iterations, not receipts for
the required testcost campaign. The topology action does not close that gap.

## Required next work

Run the real testcost time workload on CPUs 0-3 through PrismaBuild on dl380g10.
Record 30 runs for each required neighbor case.
Measure CPU 0 system or interrupt activity for every run.
Include memory-stream neighbors and record their per-CPU activity.
Preserve the idle reference and apply an explicit max/window timing rule.
Publish the receipts, table, tolerance, and final scope verdict in #1726.
On a supported no-go verdict, stop k2 to k5 and close #1541.

This issuegraph investigation needs a measurement campaign to complete.
It cannot replace that campaign with this proxy or waive the criterion.
Keep this PR unmerged until the required evidence and exact-head review exist.
