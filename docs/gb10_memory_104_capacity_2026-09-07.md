# Revised GB10 memory capacity for isolated calibration measurement

Rob authorized increasing memory limits. The root reviewer revised the earlier
96 GiB decision to 104 GiB after the capture review identified overlapping
finalization buffers; see [issue #326](https://github.com/RobTand/prismabuild/issues/326).
This is capacity for one larger whole-GPU measurement. It does not authorize
concurrent GPU work or establish that the full capture fits.

## Source-derived sizing

The unchanged collector at `prismaquant/tessera_campaign.py:1812-1819` in
`/home/rob/tmp/pq-campaign-capture-reuse` concatenates all stored X chunks while
retaining the originals, then copies all Hessians to CPU while retaining the
GPU originals. The capture review calculates a **93.7556603 GiB tensor floor**:
15.77288556 GiB checkpoint + 2 × 31.2421875 GiB H + 2 × 7.749199867 GiB X.
H is `sum(cols² * 4)`; X is `sum(min(count, 512) * cols * 4)` over 2,142 units.
The frozen census is
`/mnt/shared/tessera-measurements/first-model-20260907/full-census-512/census.json`,
SHA-256 `459b10fc2362b124211ee15edc6b75ce93553599fe52117be4e5702f03aa221c`.
This estimate excludes runtime/allocator overhead and is not a measured peak.
A 96 GiB reservation leaves only about 2.24 GiB above that tensor floor.

## Fresh headroom and admission

Read-only samples at 2026-09-07 04:59:15Z show:

| Host | MemTotal GiB | MemAvailable GiB | Broker jobs | Memory PSI some/full avg10 |
|---|---:|---:|---:|---:|
| sparky | 121.627 | 105.692 | 2 | 0.00 / 0.00 |
| sparklina | 121.627 | 115.638 | 0 | 0.00 / 0.00 |

Both broker snapshots were fresh, complete and attributed, with no foreign GPU
processes. Evidence: `/home/rob/tmp/astra-review-20260906/gb10-memory-104-headroom.json`.
Only sparklina met the 112 GiB fresh-headroom requirement in this sample;
sparky must wait for its active work to finish. Keep placement portable across
eligible GB10 workers and recheck at admission. The sample is not a standing grant.

Both versioned aggregate memory ceilings become 104 GiB, leaving about
17.627 GiB physical memory outside PB reservations. Declare `mem_gb=104`,
whole-GPU measurement isolation, actual aggregate CPU demand, and a GPU subset
cap within that same 104 GiB shared-system budget. Existing reservations must
drain. Preserve the 8 GiB host margin and all ownership/pressure gates; require
at least 112 GiB MemAvailable for this request. The GPU controller's weaker
2% free-memory reserve does not replace that requirement.

The supervisor adopts the changed versioned arguments through the normal
reviewed publication and idle-worker replacement path. No unit edits, forced
reservation release, worker interruption, or deployment is part of this change.

## Validation

The two supervisor alias/shape regression files ran through published PB on
dl380g10 with 2 CPUs / 2 GiB, two pytest workers, native threads bounded to
one, and no GPU. Action
`adbdd41940986345d6f6da0ce8350883b385058f0d07970185e1356d7ecf7c5e`
passed **11 tests, no skips**, terminal executed and return code 0. Immutable
log hashes/lengths, canonical receipt, and result payload were verified.
Receipt SHA-256: `4c140fef613967b99b7759dbb00e18c0d8809fad7346afa18cbf14ad352cb0e5`.
Audit: `/home/rob/tmp/astra-review-20260906/gb10-memory-104-validation.json`.
No GPU measurement was repeated for this configuration change.
