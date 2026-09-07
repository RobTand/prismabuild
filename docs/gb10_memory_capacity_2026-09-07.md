# GB10 memory capacity for full calibration capture

Rob authorized increasing the memory limits. The root reviewer selected a
96 GiB aggregate budget on both GB10 workers from the measured headroom,
as recorded in [issue #322](https://github.com/RobTand/prismabuild/issues/322). The previous
versioned ceilings were 72 GiB on sparky and 80 GiB on sparklina (the
`gx10-6b77` configuration entry with its explicit alias).

## Measured headroom

Read-only `/proc/meminfo`, `/proc/pressure/memory`, worker offers, and trusted
broker snapshots were read on both hosts. Linux exposes 121.627 GiB of usable
physical memory, rather than a 128 GiB allocatable budget. At 04:45–04:46Z,
both boxes had over 110 GiB available. The later snapshot records a change:

| Host | UTC sample | MemTotal GiB | MemAvailable GiB | Memory PSI some/full avg10 |
|---|---|---:|---:|---:|
| sparky | 2026-09-07 04:48:46Z | 121.627 | 95.872 | 0.00 / 0.00 |
| sparklina | 2026-09-07 04:48:46Z | 121.627 | 110.816 | 0.00 / 0.00 |

Both later broker snapshots were complete and attributed, with no foreign GPU
processes and no memory pressure. The machine-readable source is
`/home/rob/tmp/astra-review-20260906/gb10-memory-headroom.json`. Earlier active
PB reservations were 12 GiB on sparky and 16 GiB on sparklina; those belong to
other work and are not released by this change. In particular, sparky's later
95.872 GiB availability is below the 104 GiB preflight threshold for a fresh
96 GiB request plus the existing 8 GiB host margin.

## Budget and adoption

Only `--mem-gb` in `tools/fleet/fleet_boxes.json` changes, to 96 on both boxes.
That is the aggregate maximum across reservations and leaves about 25.6 GiB
of physical memory outside PB's reservation ceiling. A 96 GiB action waits
for all other memory reservations on its chosen box to drain; the limit is
not raised to 104 or 112 to permit overlap.

Keep the action portable across eligible GB10 workers, declare its actual
aggregate CPU demand, and use `mem_gb=96`. An explicit GPU memory cap of 96 GiB
is a subset of the same shared physical budget, not another 96 GiB allocation.
The declared budget must cover the model, calibration buffers, subprocesses,
and CUDA allocations together. This decision does not prove that the full
512-sample capture fits; no full capture or memory benchmark ran for it.

Fresh admission still controls placement. Require a current headroom reading
of at least 104 GiB before starting this particular request, while preserving
PB's existing host/GPU pressure and ownership gates. The GPU controller's own
minimum free-memory reserve is only max(2 GiB, 2% of physical memory), so that
weaker threshold is not substituted for the 8 GiB host margin.

The supervisor runs as Rob's systemd user service
`~/.config/systemd/user/prismabuild-supervisor.service`. It rereads the current
published `tools/fleet_boxes.json` every tick and retires only idle workers
whose arguments changed. After root review and merge, publish a new generation
through the normal idle-queue adoption path, then verify fresh offers on both
boxes and let existing work drain. This PR does not publish, chmod a sealed
generation, edit unit files, release tokens, or restart workers.

## Validation

Targeted supervisor alias/shape, host-memory observation, adaptive GPU
admission, and runtime-publication checks ran through published PrismaBuild:
action `928597fd3f0b45ff5e21eb2cddca29aa2ceead00396fc4fb5c6db808cc4569f4`,
dl380g10, 4 CPUs / 4 GiB, four pytest workers and native thread counts of one.
Result: **102 passed, no skips**, terminal `executed`, return code 0. Immutable
attempt logs, canonical receipt hash, and payload hash/byte count were checked.
Receipt SHA-256: `eda4bcc758a65d7bfc9d0a1e4ed16648bf9e17e22b1a3157614b1daf1003ed02`.
Evidence index: `/home/rob/tmp/astra-review-20260906/gb10-memory-96-validation.json`.
No GPU workload was repeated for this configuration change.

## Later same-day revision

The 96 GiB decision above is superseded by [the 104 GiB capacity decision](gb10_memory_104_capacity_2026-09-07.md), after source-derived sizing exposed overlapping finalization buffers. The earlier measurements and test receipts remain historical evidence.
