# Phase 0 guard calibration on dl380g10 (prismabuild#1726)

## Verdict

Verdict: core plus SMT sibling scope (8 threads), GO. Same-node load leaves
probe timing clean, so no wider scope is needed. k2 to k5 continue.

## Probe

Probe mimics the testcost timing capture at short scale. Full capture
`5a4dcf687cb4` runs 876 s wall with 4 workers on CPUs 0-3 (4 CPUs, 9 GB).
Its telemetry shows user CPU 3444 s against system 17 s with peak memory
593 MB. 150 full runs would hold the host 35 h. The proxy keeps the same
shape: 4 parallel workers on CPUs 0-3, CPU-bound sha256 plus streaming
memory copy, CPU 0 included. One run takes about 2.6 s.

Probe command per worker:

```bash
taskset -c {0,1,2,3} /home/rob/venvs/pb-cpu/bin/python -c \
  "data=os.urandom(1048576); [sha256(data) for _ in range(300)]; \
   src=bytearray(os.urandom(64*1048576)); [copy src for _ in range(100)]"
```

Campaign command (15 interleaved rounds, 5 cases per round, 75 runs):

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --cwd CHECKOUT --tag dl380g10 --tag x86 --cpus 8 --demand mem_gb=4 \
  --measurement --env OMP_NUM_THREADS=1 --env TMPDIR=/tmp --timeout-s 1800 \
  --detach -- /home/rob/venvs/pb-cpu/bin/python phase0_main.py {1,2} 15
```

CPU sets: probe always 0-3. Neighbors: idle none, smt 40-43, samenode 4-7,
othernode 10-13, othersocket 20-23. Round order is shuffled with a fixed
seed. Two actions (seeds 1 and 2) repeat the full set.

## Topology (sysfs on dl380g10)

Xeon Gold 6230, 2 sockets, 20 cores per socket, 2 threads per core.
Node cpulists: node0 0-9,40-49; node1 10-19,50-59; node2 20-29,60-69;
node3 30-39,70-79. SMT siblings pair N with N+40 (cpu0 shows 0,40).
Package ids: cpu0 and cpu10 on 0, cpu20 on 1. Probe 0-3 sits on socket 0
node 0. Cases cover SMT sibling, same node, other node same socket, and
other socket.

## Tolerance

Tolerance source is the host idle baseline judged by `_judge_idle`.
Path: `/tmp/prismabuild-admission-1000/693d63.../idle-baseline.json`,
identity 80, 61 to 62 samples, span about 304804 s, window bound 256.
`busy_cpus`: mean 10.03, max 33.683642, margin 23.65, stdev 5.54.
`psi_some`: mean 0.0134, max 0.120927, margin 0.1076, stdev 0.0199.
A run exceeds when it tops the max on any field. 0 of 150 runs exceed.

Idle probe timing (30 runs): wall mean 2.5798 max 2.9805, CPU mean 0.8761
max 1.0948, memory mean 1.3834 max 1.5371.

## Table (30 runs per case, 15 per action)

| case | wall mean | wall max | CPU mean | CPU max | mem mean | mem max | host over max | psi over max | cpu0 busy |
|---|---|---|---|---|---|---|---|---|---|
| idle | 2.5798 | 2.9805 | 0.8761 | 1.0948 | 1.3834 | 1.5371 | 0/30 | 0/30 | 0.9973 |
| smt 40-43 | 3.3621 | 3.5981 | 1.6287 | 1.6778 | 1.3667 | 1.6259 | 0/30 | 0/30 | 0.9990 |
| samenode 4-7 | 2.5908 | 2.9841 | 0.8822 | 1.3272 | 1.3815 | 1.5509 | 0/30 | 0/30 | 0.9981 |
| othernode 10-13 | 2.5758 | 2.7835 | 0.8826 | 1.0024 | 1.3755 | 1.4887 | 0/30 | 0/30 | 0.9971 |
| othersocket 20-23 | 2.5634 | 2.6390 | 0.8754 | 1.0079 | 1.3750 | 1.4570 | 0/30 | 0/30 | 0.9982 |

Repeat check: seed 1 and seed 2 agree. SMT CPU means are 1.6159 and
1.6415 against idle 0.8720 and 0.8802. Other cases match idle in both
seeds. The earlier other-node exceedances do not repeat, so they were
time-correlated ambient load. Lone maxima (samenode 1.3272, idle 1.0948)
strike idle too, so they are noise.

## Receipts

| action | seed | result | returncode | host | elapsed |
|---|---|---|---|---|---|
| `b5c2710eed003e131529fce10f6a746a5e6d29c7d902e5ded900a98ad4250145` | 1 | executed | 0 | dl380g10 | 221 s |
| `f130852dc89e8cbf5c056af0af3152155e6fe55ed5b2a4d89ac3115466f66e19` | 2 | executed | 0 | dl380g10 | 223 s |

Done records live under `/mnt/shared/prismabuild-fleet/pb-queue/done/`.
Pilot topology action `154b6820058bba8be065b20d4175b313d2500008e828f98e125ec122eae457a9`
also passes on dl380g10.
