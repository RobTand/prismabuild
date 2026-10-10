# GPU closure device-use signal: decision note (prismabuild#1743)

Serves parent prismabuild#1740 criterion C2. No product code changes. All paths and lines cite `origin/main` at `561e3177be3c8e6f3e21e2f7ffa5abddf0c91663`. The worktree HEAD equals that sha.

## Signal source (one)

The one signal source is the in-run device stamp in the adopted attempt stdout. The stamp holds two facts:

- `TORCH <torch-ver> CUDA <cuda-ver> DEVICES <n>`, where `n` is `torch.cuda.device_count()` inside the run.
- `KERNEL <value>`, where `value` proves a real device tensor op ran.

The proven pattern is `tools/fleet/pbcanary_legs/leg2.py`:

- `_inner_script` (line 66) prints the stamp (lines 76-78), refuses zero devices (lines 79-81), then runs the kernel op (lines 82-84).
- `verify` (line 134) fails `DEVICES < 1` (lines 158-162) and fails a missing kernel line (lines 163-167).
- `DEMAND` (line 59) seals `gpu: 1`, so the stamp belongs to a GPU-demanded run.

The closure reads the stamp from the adopted attempt, not from host telemetry. `PoolQueue.adopted_attempt_summary` (in `src/prismabuild/pool.py`, line 26371) selects the immutable winner. `outcome_summary` (in `tools/fleet/pbrun.py`, line 3453) reduces the ending to reported fields. `await_outcome` prints attempt stdout (lines 4024-4031) and maps the outcome to an exit status (lines 4042-4049).

Fields read for each GPU-demanded run:

- `attempts[].stdout`: the `DEVICES n` line and the `KERNEL` line.
- `detail.returncode` / `detail.action_returncode`: the exit-zero gate.
- Sealed `demand.gpu`: the GPU-run gate.

## Fake run records (zero versus one or more)

Zero-device record (must refuse when `returncode` is 0):

- `stdout`: `TORCH 2.7.0 CUDA 12.6 DEVICES 0` with no `KERNEL` line.
- `detail.returncode`: `0`. Sealed `demand.gpu`: `1`.
- Read result: `n = 0`, kernel absent. Verdict: refuse.

One-or-more-device record (closes as before):

- `stdout`: `TORCH 2.7.0 CUDA 12.6 DEVICES 2` plus `KERNEL 1240`.
- `detail.returncode`: `0`. Sealed `demand.gpu`: `1`.
- Read result: `n = 2`, kernel present. Verdict: close with exit 0.

These shapes are executable today. `tests/test_pbcanary_legs12.py` builds them in `_leg2_artifact` (lines 132-140), passes with devices set (lines 143-149), and fails visibility with zero devices (lines 152-157).

## Closure site that must refuse

File: `tools/fleet/pbrun.py`. Function: `await_outcome` (line 3792). It blocks until the pool action lands, renders the summary, prints the headline (line 4042), and returns 0 for `executed` / `cache_hit` / `returncode` 0 (lines 4043-4049). k3 adds the refusal there: a GPU-demanded exit-zero run whose stamp shows zero devices (or a missing kernel line) prints a refusal that names the reason and returns nonzero instead of 0. Field reduction stays in `outcome_summary` (line 3453). A run with no stamp closes as before; absence is a recorded gap, not a refusal.

## CPU routing site for a device-free module

File: `tools/fleet/pbrun.py`. Function: `placement_contract` (line 1747). It derives placement tags. A device-free module seals no GPU demand and no CUDA path requirement, so it receives no `gb10` tag (compare the `gb10` derivation at lines 1779-1782) and never queues to the gb10 population. The liveness check reuses the already open bounded reader: `bounded_attachment` (line 3760) discovers attachment in a finite child, and the submission path calls it before publication (lines 9207, 9243). k3 reuses that delivered snapshot for the routing check (same pattern as `use_delivered_snapshot=True` at lines 3874-3884) instead of opening a fresh queue read.

## Rejected alternatives

- Sealed `CUDA_VISIBLE_DEVICES` and sealed `demand.gpu`: they state submit-time intent, not executed-on fact. `pbrun.py` masks the variable for CPU slots (lines 7976-8001). The D38 design reads device hiding from the sealed environment only (in `docs/design.md`, lines 17515-17519).
- Broker and cgroup telemetry (`src/prismabuild/resource_scope.py`, lines 406-410; `PoolQueue._resource_profile` in `src/prismabuild/pool.py`, line 28697): they carry CPU, memory, and I/O totals. They carry no per-action device counter.
- Host GPU power (`src/prismabuild/box_capacity.py`, lines 376-397; `resource_profile_summary` in `src/prismabuild/pool.py`, line 3814): it is host-level. The placement proxy reads 1.0 on a throttled idle device, so it cannot tell zero use from idle.
- Per-process `nvidia-smi`: it reads null on GB10 (`src/prismabuild/slurm_lane.py`, lines 1626-1633). The node-side power seam returns `None` today (line 1712).

## New record field

No new record field is needed. The payload program writes the stamp. The worker files attempt stdout unchanged. The CAS-bound artifact already carries the same bytes (`verify` checks the sha256 binding at `leg2.py` lines 168-176). If k3 later wants a first-class field, the writer that adds it is the GPU payload program, not the worker and not the broker.

## Final file list for k3

Copy this list into the k3 PR:

- `tools/fleet/pbrun.py`
- `tests/test_pbrun_bounded_attachment.py`
- `docs/design.md`
- This note: `docs/gpu_closure_device_signal_2026-10-10.md` (reference; already landed)
