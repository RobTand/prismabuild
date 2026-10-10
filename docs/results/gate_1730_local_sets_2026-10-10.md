# Gate 1730: local-set speed, space, bind mount (2026-10-10, re-measure)

Question: does a local A8S copy on a Spark win for A8S?
Set: `/mnt/shared/tessera-runs/moe/glm53-a8-bf16menu-20260930/release/exported`, 175527382717 bytes (163.47 GiB), 128 files.
Method: `tools/fleet/gate_1730_probe.py` (schema v2) ran as a PB-admitted action on each Spark.
Actions: sparky `4c034a6c16e47d2d704912881042f4711ae4638e2c3c92925a2019eb3bbdedef`, sparklina `5d0a66beabc10df12f2bcaa52a371794103f21ca2afbac36677ca2301b8e4d7b`.
Raw reports: `docs/results/gate_1730_sparky_2026-10-10_v2.json`, `docs/results/gate_1730_sparklina_2026-10-10_v2.json`.
Host pin reason: disk free, NFS paths, and Docker binds are host-local facts.
This re-measure supersedes the v1 speed arms. The v1 files stay in this directory for audit.

## What changed after review

The review of the first measure found four faults. This run fixes each one:

1. The NVMe arm now matches the NFS arms: 16 distinct files, same 8 GiB total, same 1/4/16 streams.
2. Every arm reads 16 files with one file per thread at 16 streams. No thread idles (`idle_threads: 0` on all 36 arms).
3. Cold now means client-cold: the client drops its page cache before each cold arm, and the 16-stream cold arm touches the bytes first. The server ARC/L2ARC stays outside client reach. The probe tries `/proc/spl/kstat/zfs/arcstats` and records `null` on the Sparks. So NFS cold rates are upper bounds, not disk rates.
4. The decision below rests on space alone. It makes no repay claim from speed.

A `--measurement` action was not used. It pins the submitter host class, so it cannot target one Spark from the coordinator. All arms run back to back on one host. Each arm records its load average.

## Speed (MiB/s, fixed 8 GiB arm: 16 files x 512 MiB prefix)

sparky, stage tier (`/stage/prewarm`, NFS ro):

| streams | cold | warm |
|---|---|---|
| 1 | 1865.8 | 1440.3 |
| 4 | 5984.8 | 5969.7 |
| 16 | 2291.9 | 7859.6 |

sparky, HDD pool (`/mnt/shared`):

| streams | cold | warm |
|---|---|---|
| 1 | 1540.5 | 1531.8 |
| 4 | 4417.0 | 4502.6 |
| 16 | 6114.1 | 6215.6 |

sparky, host-local NVMe (16 written files, varied pattern, fsync, backing verified non-sparse, write 1274.9 MiB/s):

| streams | cold | warm |
|---|---|---|
| 1 | 3001.7 | 2992.4 |
| 4 | 7042.0 | 6701.2 |
| 16 | 7689.8 | 7805.2 |

sparklina, stage tier:

| streams | cold | warm |
|---|---|---|
| 1 | 1673.3 | 1704.3 |
| 4 | 6132.0 | 6071.5 |
| 16 | 5904.6 | 7377.7 |

sparklina, HDD pool:

| streams | cold | warm |
|---|---|---|
| 1 | 4081.7 | 5520.1 |
| 4 | 6476.5 | 5762.0 |
| 16 | 1079.1 | 6695.0 |

sparklina, host-local NVMe (backing verified non-sparse, write 2609.7 MiB/s):

| streams | cold | warm |
|---|---|---|
| 1 | 1940.6 | 2063.1 |
| 4 | 4237.8 | 4302.8 |
| 16 | 6622.2 | 6558.4 |

Read these numbers with care:

- The 16-stream cold arm ran first on each source, so it is the closest to a true cold read. Later cold arms re-touch server-warmed bytes.
- Sparklina HDD 16-stream cold (1079.1) against warm (6695.0) shows the gap between a server miss and a server hit. The v1 "cold" arms hid this gap by re-reading one 8 GiB set twelve times.
- Load stayed low on both hosts during all arms (about 5.5 on sparky, 5.5 to 7.5 on sparklina; per-arm values are in the raw JSON). The fleet was near idle, so NFS warm rates are best cases.
- No full-set extrapolation follows. A rate over an 8 GiB prefix does not predict a 163 GiB load.

## Space (D1 verdict for the 163.47 GiB set)

D1 needs 1.5N + 20 GiB free (265.21 GiB here) plus the 5% floor.

| host | avail GiB | floor GiB | verdict |
|---|---|---|---|
| sparky | 559.95 | 91.66 | fits |
| sparklina | 109.88 | 45.77 | fails (1.5N+20GiB rule) |

Cleanup landed before this probe. Sparklina still cannot hold the set.

## Bind mount

Image `localhost/prismaquant/spark-vllm-nccl230:nightly-20260929` on both hosts.
A `--mount type=bind,src=<local>,dst=/mnt/shared/gate-1730-<host>,readonly` inside a container that already has `-v /mnt/shared:/mnt/shared:ro` shows only the local files (`nested_shows_local_only: true`, rc 0).
`--mount` with a missing source fails (rc 125, clear error), so it has no silent empty-directory trap.
This leg passes on both hosts.

## Decision: NO-GO for the A8S local copy, on space alone

Sparklina fails space, so a two-host resident set is impossible now. That alone stops k1 shim work per the issue.
The speed arms are evidence, not a verdict: local NVMe at 16 streams reads cold at 7689.8 MiB/s (sparky) and 6622.2 MiB/s (sparklina), against first-touch NFS reads of 2291.9 to 6114.1 MiB/s. Warmed NFS matches or beats local NVMe. No repay claim follows, because the loader concurrency of A8S is not measured here and server-cache state dominates NFS rates.
The bind-mount technique stays valid for any later set that passes this gate.
Revisit only with new evidence: more free space on sparklina, the real vLLM loader concurrency for A8S, or NFS rates under campaign load.
No queue, shim, or policy code changed in this task.
