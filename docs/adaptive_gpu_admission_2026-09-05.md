# Adaptive GPU admission implementation evidence — 2026-09-05

The controller uses one physical GPU on each GB10 host, fresh broker device
and attempt evidence, and one incremental concurrency probe per sample. Host
memory tokens and CPU affinity are unchanged. GPU reservation metadata retains
legacy demand, exclusivity, device UUID and the independent GPU memory budget.
The normative policy is in [design.md](design.md#adaptive-gpu-admission-and-independent-memory-domains).

## Regression and focused validation

All execution used the published PrismaBuild runtime on dl380g10, Python 3.14.4,
CPU only, eight pytest workers, eight declared CPUs and eight GiB total memory.
`OMP_NUM_THREADS=1` and `OPENBLAS_NUM_THREADS=1` bounded native threads.

The initial regression on base `ffb260f` failed twice: both historical GPU slot
counts 2 and 3 admitted a second cold-start action without GPU evidence.
Action: `838ea921815425ec08b56bbebe794249de7094cf67b1bcef85a435c2ed4626c4`.
Terminal: `/mnt/shared/prismabuild-fleet/pb-queue/failed/838ea921815425ec08b56bbebe794249de7094cf67b1bcef85a435c2ed4626c4.json`.
Return code 1; two expected assertion failures; no success receipt.

Final focused command:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --cwd /home/rob/prismabuild-adaptive-gpu --tag dl380g10 \
  --cpus 8 --demand mem_gb=8 --detach \
  --env OMP_NUM_THREADS=1 --env OPENBLAS_NUM_THREADS=1 -- \
  /home/rob/venvs/pb-cpu/bin/python -m pytest \
  tests/test_adaptive_gpu.py tests/test_adaptive_cpu.py \
  tests/test_pool_claimant_private_reservation.py tests/test_pool_resource_scope.py \
  tests/test_pool_scope_creation_recovery.py tests/test_pbrun_gpu_contract.py \
  tests/test_pbrun_declares_cores.py tests/test_pbrun_cpu_slot_has_no_gpu.py -q -n 8
```

Result: **113 passed in 2.07 seconds**, no skips, terminal `executed`, return code 0.
Action: `3e63af2b938983e98be79a4f799dc762630ee67666ccf21bb243efb5fb8f4aae`.
Receipt: `/mnt/shared/prismabuild-fleet/cas/actions/v3/3e/3e63af2b938983e98be79a4f799dc762630ee67666ccf21bb243efb5fb8f4aae.json`.
Receipt digest: `02e90907a66a5bd3d21635307df85a23d92edbb1132d3326d55ad7664abd5652`.
Payload: `/mnt/shared/prismabuild-fleet/cas/blobs/15/15f2ed03f20aaabcf9f5790f387bfc5d279b9806f137bd8ec026497191dfec6d`.
The payload hash and byte count were verified against the receipt.

Coverage includes old-slot independence; concurrency above physical tokens;
unchanged RAM limits; pressure and missing-evidence refusal; startup pacing;
hysteresis; concurrent claimants; durable spent-sample credit; release and
abandonment without token creation; exclusive/measurement admission; legacy
multi-slot normalization; independent discrete VRAM reservation and release;
and sealed producer options and rejection paths.

## Scope of evidence

These are scheduler and producer tests with modeled GPU telemetry. They do not
measure useful GPU throughput, power efficiency, physical memory enforcement or
real discrete GPU behavior. The sibling broker/device telemetry and memory guard
changes must be integrated before live qualification. Explicit scope VRAM budget
creation/recovery depends on that sibling ResourceScope protocol extension.
Multi-device worker allocation remains unsupported by this controller.

## Follow-up: measured plateau below the SoC power reference

The first controller's low-power permission alone was insufficient. The sibling
readiness experiment ran the same 20.48 million BF16 embeddings on Sparklina
with one context and two contexts; the two-context starts differed by 7.8 ms.
One context took 44.356 seconds, two took 45.997 seconds: useful throughput fell
3.57%. Sampled GPU energy rose from 3408.7 J to 3570.2 J (4.74%). Mean sampled
steady power was 78.86 W versus 79.39 W, both below the initial 91 W probe
threshold. CUDA event time per batch rose from 4.006 ms to 7.117–7.937 ms.
The one-context samples included a 43.13 W outlier; it was retained.

Measured summary inspected:
`/home/rob/pb-logs/gpu-adaptive/before-compute-curve-synchronized.json`.
Raw run directories:
`/home/rob/pb-logs/gpu-adaptive/before-compute-one-sparklina-119e759266` and
`/home/rob/pb-logs/gpu-adaptive/before-compute-two-barrier-sparklina-2ccce530b6`.
This qualifies the need for a plateau gate on that workload; it does not prove
that power is a universal throughput metric. NVML GPM activity metrics were
reported unsupported on both GB10 devices by the telemetry agent.

The controller now waits for three fresh post-startup observations after a
probe. It compares the mean power response with observed standard error and a
relative deadband. No measurable response latches a plateau; the latch persists
across restart and individual holder departure, so unnecessary concurrency can
fall naturally. A sustained power reduction or the end of the whole busy period
reopens exploration. Old samples cannot bridge a telemetry gap.

The three plateau regressions were run against `020b8a6` with only the new tests
in `/home/rob/prismabuild-gpu-plateau-red`, retained as bounded regression evidence.
All three failed by admitting another action at a plateau/noisy response.
PB action: `70dabbfabc388be410aaebf3a7dfd8c5d5bdab332f8cf5008be42d9ca22cb0e3`;
terminal failed, return code 1, no success receipt.

After the change, the focused GPU/CPU admission, claimant reservation and GPU
producer contract suite passed **86 tests in 1.99 seconds**, no skips, on the
same dl380g10 CPU-only eight-worker/eight-GiB PrismaBuild mode.
PB action: `052bc056bff2945212cc0a20a3b266040a12f77c37b600eae15d0b70650798e2`.
CAS receipt: `/mnt/shared/prismabuild-fleet/cas/actions/v3/05/052bc056bff2945212cc0a20a3b266040a12f77c37b600eae15d0b70650798e2.json`.
The integrated live low-duty AFTER workload must still establish whether the
controller's more conservative feedback cadence improves useful throughput.
