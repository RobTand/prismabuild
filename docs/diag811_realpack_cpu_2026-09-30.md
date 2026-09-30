# Checkout-cache prototype: corrected paired CPU measurement

## Scope

This is a real-pack qualification of the experimental harness after #1372.
It is not a production cache, a deployment record, or completion of #811.
No worker runtime, default, retention rule, or staged-read ledger status changed.

The comparison uses the same retained 32,899,482-byte Git bundle in both arms:

- **A:** the published generation's existing checkout materializer, then verification.
- **B:** the warmed experimental pack/index entry, independent private copies,
  checkout, then verification.

Both timed arms include materialization and verification. Cleanup, population,
cold entry verification, negative controls, and parity are outside that boundary.
The earlier v1 boundary omitted B's verification and is not the comparison here.
This does not measure the end-to-end latency of the original 8,356-byte result.

## Execution and binding

- PB action: `0d49a407ed522b703e4d898c76fabac3729147c5c15f8d8980da71169ef61e8f`.
- One attempt on **dl380g10, Linux x86_64**, terminal `executed`, return code **0**.
- Python **3.14.4**, Git **2.53.0**, no GPU; `CUDA_VISIBLE_DEVICES` was empty.
- `--measurement --host-class dl380g10 --tag dl380g10 --profile sample`,
  priority **-10**, CPU **2**, memory **2 GiB**, native threads **1**,
  execution deadline **1200 s**. This was one action, not a fanout.
- PB assigned preferred CPUs **0, 1**, with no fallback CPUs. The harness did not
  replace that affinity. Worker execution elapsed **59.5020 s**.
- Source parent: `03374e98c267bda33904be9c329fd0c5da51b683`;
  measured harness SHA-256:
  `9ca0134e05099266c817e2b93d0326fd6c26a7db778b14e387be11b4c177364f`.
- Published runtime generation: `3aff9642ab39-1790654284-1eed70850170`.
- Retained bundle SHA-256:
  `97bb30f295dfe4701e2ed1e64f2ca662fff25bcf322d8b67b43859ada78e983f`;
  sealed checkout: `329a1c82ad596a656d92fb49e12e80e066b321a6`.
- Read-only post-run `findmnt` resolved both scratch parents to
  **btrfs on /dev/nvme1n1p4**. The run reported device 31 and a 128 GiB
  filesystem. This is filesystem context, not a sealed mount-identity proof
  or a claim about another tier or host.

PB admitted the action against its measured idle envelope: 256 samples over
31,091.08 s; busy-CPU mean **5.779397**, maximum **9.81638**; admission observed
**5.740642** busy CPUs and PSI-some **0.007505**, `exceeds: false`, state `idle`.
The host was not literally empty. Matched Netdata retained background NFS work.

## Paired result

Seconds, with four alternating pairs:

| Pair | Order | A: existing materializer + verify | B: warmed prototype + verify |
|---|---|---:|---:|
| 0 | AB | 6.4165 | 1.4240 |
| 1 | BA | 5.8793 | 1.3950 |
| 2 | AB | 5.6925 | 1.0363 |
| 3 | BA | 6.3522 | 1.3645 |
| Median | — | **6.1158** | **1.3798** |

For this warmed prototype and this boundary, the ratio of medians is **4.4324**
(77.44% less elapsed time). It is not a production speedup or a cold-cache result.
Population cost **4.9038 s** and cold entry verification cost **4.6543 s** are
separate; no break-even or retention policy is inferred from four pairs.

Median phases include A materialization **5.3809 s**, A verification **0.7272 s**,
and B verification **0.6970 s**. B object copies used reflinks. The complete
phase and per-repetition data are in the CAS payload linked below.

## Correctness and coverage

The real cold entry passed the repaired publication manifest binding, bundle
and pack hashing, and Git index verification. Pack bytes were unchanged by
index creation. All three real corruption controls refused their input:

- tampered pack hash;
- tampered pack Git verification;
- tampered index Git verification.

Parity passed for **2,763 tracked paths** in each arm: tree bytes, index, refs,
status, HEAD, FETCH_HEAD, checkout identity, and both preflights agreed.
The parity fetch added no objects. These are harness controls, not pytest counts.
The synthetic publication regression matrix remains #1372's separate evidence:
RED **11 failed / 1 positive control passed / 0 skipped**, final GREEN
**14 passed / 0 failed / 0 skipped**, with no collection gaps.

PB's py-spy **0.4.2** profile sampled at **100 Hz**, produced **602 samples**,
and reported no sampling errors. The profile covers both measured arm wrappers
(`arm_a_rep`: 121 samples; `arm_b_rep`: 184 samples). These are inclusive active
Python samples, not wall-time shares: native Git time and inactive waits are not
fully represented. The profile does not establish a native-kernel attribution.

Matched Netdata has **62 rows per chart**, spanning requested seconds
**1790744321–1790744383**, for CPU, load, block I/O, NFS I/O, NFS RPC, and NFSv4
operations. The worker's box-window CPU mean was **9.3453%**, peak **14.2479%**.
The separately retained RPC series averaged **27,019.8 calls/s**. These are
whole-host observations, not demand charged to this action. Worker telemetry
reported no covering pqteld CSV; there is no GPU, power, or energy claim.

## Durable evidence

The published generation's `PrismaBuildCAS.lookup_execution` independently
validated the execution receipt, action binding, producer attestation and result
blob. Immutable attempt stdout/stderr lengths and hashes also matched. The
profile blob's length and SHA-256 were checked separately.

| Evidence | SHA-256 | Bytes |
|---|---|---:|
| Execution receipt | `ed747b7c6e1980603d16b58e8729987243149c72fcf104b02a0e6cf16fdf0451` | — |
| CAS result | `831b55d549004f854d4bf40e1e31531ab8c205153fb64760390ce6da6af38b20` | 24,194 |
| Profile | `4e6fa8bd4b5cbf53237fb5631e4e427856fe174ce5a55551af0bf24c2a1b669a` | 50,152 |
| Immutable worker stdout | `3074992d2c5c0b2f2a926604075f99d440f0cead8c944ee35c3965010361b086` | 28,164 |
| Matched Netdata capture | `1a038776a29c8408182d4c85d75109a1c27ab22c676c96120f35f3ba01bea129` | 37,191 |

CAS blobs are under `/mnt/shared/prismabuild-fleet/cas/blobs/<first-two>/<digest>`.
The result contains the `E1-REPORT-BEGIN` / `E1-REPORT-END` JSON block; it is
not itself a JSON file. The matched observation is archived in
[`measurements/diag811_netdata_2026-09-30.json`](measurements/diag811_netdata_2026-09-30.json).
Its digest binds the saved observation, not a worker attestation.

## Remaining production gaps

This result does not qualify production cache lifetime, charge accounting,
retirement, crash recovery, competing readers, cross-generation reuse, or other
filesystems. It does not deploy a cache or authorize bulk payload fallback.
Those decisions and integration checks still belong to #811 and the staged-read
contract. #940 still needs a live eligible holder-bound claim-pass profile;
this checkout experiment is not its missing witness. Tessera's held GPU arms
are unaffected.
