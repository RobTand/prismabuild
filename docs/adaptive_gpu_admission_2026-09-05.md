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
