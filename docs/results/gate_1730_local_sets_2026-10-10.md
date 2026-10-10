# Gate 1730: local-set speed, space, bind mount (2026-10-10)

Question: does a local A8S copy on a Spark win for A8S?
Set: `/mnt/shared/tessera-runs/moe/glm53-a8-bf16menu-20260930/release/exported`, 175527382717 bytes (163.47 GiB), 128 files.
Method: `tools/fleet/gate_1730_probe.py` ran as a PB-admitted action on each Spark.
Actions: sparky `5eec338c622561c0ecc288b7f8ebf884fb1bf4ae858061f08a0ac94091c46827`, sparklina `a03b6473accc76a8195b22170f98aea8e57f58cd51936bd65ecd5a79f30985fc`.
Raw reports: `docs/results/gate_1730_sparky_2026-10-10.json`, `docs/results/gate_1730_sparklina_2026-10-10.json`.
Host pin reason: disk free, NFS paths, and Docker binds are host-local facts.

## Speed (MiB/s, ~8 GiB subset, cold = client cache dropped, warm = re-read)

sparky, stage tier (`/stage/prewarm`, NFS ro):

| streams | cold | warm |
|---|---|---|
| 1 | 764.7 | 1294.8 |
| 4 | 4259.8 | 4431.2 |
| 16 | 6263.8 | 6229.1 |

sparky, HDD pool (`/mnt/shared`):

| streams | cold | warm |
|---|---|---|
| 1 | 1444.1 | 1531.1 |
| 4 | 2520.2 | 2540.6 |
| 16 | 3201.9 | 3084.3 |

sparklina, stage tier:

| streams | cold | warm |
|---|---|---|
| 1 | 1608.7 | 1665.6 |
| 4 | 4155.9 | 4206.5 |
| 16 | 5848.1 | 5564.8 |

sparklina, HDD pool:

| streams | cold | warm |
|---|---|---|
| 1 | 1031.8 | 2672.0 |
| 4 | 3975.3 | 3978.2 |
| 16 | 3755.6 | 3956.4 |

Host-local NVMe (2 GiB scratch in `/var/tmp`, fsync write):

| host | write | cold read | warm read |
|---|---|---|---|
| sparky | 1903.0 | 1517.3 | 25798.6 |
| sparklina | 4048.8 | 6098.1 | 15968.1 |

Load stayed low on both hosts during all arms (see raw JSON).
The fleet was near idle, so these are best-case NFS rates.

## Space (D1 verdict for the 163.47 GiB set)

D1 needs 1.5N + 20 GiB free (265.21 GiB here) plus the 5% floor.

| host | avail GiB | floor GiB | verdict |
|---|---|---|---|
| sparky | 560.10 | 91.66 | fits |
| sparklina | 110.80 | 45.77 | fails (1.5N+20GiB rule) |

Cleanup landed before this probe. Sparklina still cannot hold the set.

## Bind mount

Image `localhost/prismaquant/spark-vllm-nccl230:nightly-20260929` on both hosts.
A `--mount type=bind,src=<local>,dst=/mnt/shared/gate-1730-<host>,readonly` inside a container that already has `-v /mnt/shared:/mnt/shared:ro` shows only the local files (`nested_shows_local_only: true`, rc 0).
`--mount` with a missing source fails (rc 125, clear error), so it has no silent empty-directory trap.
This leg passes on both hosts.

## Decision: NO-GO for the A8S local copy

Sparklina fails space, so a two-host resident set is impossible now.
Sparky fits space, but speed does not repay the copy.
At 16 streams the HDD pool loads the full set in about 52 s (3201.9 MiB/s).
Sparky local NVMe cold reads at 1517.3 MiB/s, so a local load takes about 112 s.
The local copy is slower than the network it replaces.
Copy cost from the stage tier is about 2 to 4 min at measured 1-stream rates.
A single-stream consumer saves about 8 s per reload on sparky, which never repays the copy.
Sparklina local load takes about 27 s against about 43 s over 16-stream NFS. The 16 s saving per reload needs 13 or more reloads to repay one copy, and the host cannot hold the set.
These rates are idle-fleet best cases for NFS.
A loaded fleet could change the balance, but no current number supports the copy.

## What stops and what stays open

k1 shim work stays stopped: the gate blocks it per the issue.
The bind-mount technique stays valid for any later set that passes this gate.
Revisit only with new evidence: the real vLLM loader concurrency for A8S, or NFS rates under campaign load, or more free space on sparklina.
No queue, shim, or policy code changed in this task.
