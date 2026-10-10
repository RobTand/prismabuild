# GPU closure device-use signal: decision note (prismabuild#1743)

Serves parent prismabuild#1740 criterion C2. No product code changes. All paths and lines cite `origin/main` at `561e3177be3c8e6f3e21e2f7ffa5abddf0c91663`. Revision 2 corrects three review findings: the CPU routing site, the universal signal writer, and the zero-device example.

## Signal source (one)

The one signal source is a worker-observed device count on the attempt record. The pool worker writes it at execution time. The closure reads it from the adopted attempt. One field answers zero versus one or more devices for every GPU run.

A new record field is needed. No existing per-action field carries executed-on device fact. Sealed `demand.gpu` and sealed `CUDA_VISIBLE_DEVICES` state submit-time intent, not fact. Broker and power telemetry are host-level. The canary stamp covers canary runs only (see below).

## Writer that emits the signal for every GPU run

File: `src/prismabuild/pool.py`. Function: `PoolQueue._execute_in_checkout` (line 28820). It is the only code that runs the action as a child on the executing box. It builds the attempt `outcome` dict (lines 29559-29570: `status`, `returncode`, `stdout`, `stderr`, `elapsed_s`, `argv`) and files it through `ending(outcome)` (line 29576). k3 adds the device count beside `returncode` there.

The probe runs in the action's sealed launch environment. `run_local_action` (in `src/prismabuild/core.py`, line 7965) builds that environment (lines 8066-8087) and execs the sealed argv in it. The probe shares that environment, so it sees the same devices the payload sees. k3 designs the probe mechanics; this note fixes the seam only.

Filing and reading path, already built:

- `archive_attempt` (in `src/prismabuild/pool.py`, line 25719) files attempt details first-writer-wins.
- `attempt_outcomes` (in `src/prismabuild/pool.py`, line 25997) reads the immutable linked attempts.
- `adopted_attempt_summary` (in `src/prismabuild/pool.py`, line 26371) selects the immutable winner.
- `outcome_summary` (in `tools/fleet/pbrun.py`, line 3453) reduces the ending to reported fields.

Fields the closure reads for each GPU-demanded run:

- New worker field `devices` on the adopted attempt detail: the executed-on count.
- `detail.returncode` / `detail.action_returncode`: the exit-zero gate.
- Sealed `demand.gpu` / `needs_gpu`: the GPU-run gate.

## Canary stamp is the pattern, not the source

`tools/fleet/pbcanary_legs/leg2.py` proves the stamp shape, and only that:

- `_inner_script` (line 66) prints the stamp (lines 76-78), refuses zero devices (lines 79-81), then runs the kernel op (lines 82-84).
- `verify` (line 134) fails `DEVICES < 1` (lines 158-162) and fails a missing kernel line (lines 163-167).
- `DEMAND` (line 59) seals `gpu: 1`, so the stamp belongs to a GPU-demanded run.

Real GPU runs print no such stamp. A refusal that reads only the canary stamp would never fire for them. The worker field covers every run because the worker launches every run.

## Run records (zero versus one or more)

The payload below is an ordinary GPU-demanded torch job with no self-probe. It takes the CPU path when no device is visible and exits 0. It prints no stamp. `leg2.py` cannot produce the zero-device record: it exits 11 on zero devices (lines 79-81), so `DEVICES 0` with `returncode` 0 never comes from it.

Zero-device record (must refuse):

- `stdout`: application output only, no stamp.
- `detail.returncode`: `0`. Sealed `demand.gpu`: `1`. New worker field `devices`: `0`.
- Read result: exit-zero GPU-demanded run with zero executed-on devices. Verdict: refuse.

One-or-more-device record (closes as before):

- `stdout`: application output only, no stamp.
- `detail.returncode`: `0`. Sealed `demand.gpu`: `1`. New worker field `devices`: `1`.
- Read result: exit-zero GPU-demanded run with one executed-on device. Verdict: close with exit 0.

Test note: `tests/test_pbcanary_legs12.py` builds stamp shapes in `_leg2_artifact` (lines 132-140) and `test_leg2_verify_zero_devices_fails_cuda_visibility` checks `verify`, not `await_outcome`. These tests evidence stamp semantics only. The refusal test belongs to k3 and targets `await_outcome`.

## Closure site that must refuse

File: `tools/fleet/pbrun.py`. Function: `await_outcome` (line 3792). It blocks until the pool action lands, renders the summary, prints the headline (line 4042), and returns 0 for `executed` / `cache_hit` / `returncode` 0 (lines 4043-4049). k3 adds the refusal there: a GPU-demanded exit-zero run whose worker `devices` field reads zero returns nonzero instead of 0 and names the reason. Field reduction stays in `outcome_summary` (line 3453). A pre-field record with no `devices` field closes as before; absence is a recorded gap, not a refusal.

## CPU routing site for a device-free module (corrected)

Correction: `placement_contract` (in `tools/fleet/pbrun.py`, line 1747) does not route to CPU. It derives the `gb10` tag from hostname membership or `celestia` plus `needs_gpu` (lines 1779-1782). It has no CPU route. k3 changes nothing there.

The real route is demand sealing plus the claim gate:

- The submitter declares demand. `--gpu` (in `tools/fleet/pbrun.py`, lines 7213-7214) sets `demand.setdefault("gpu", 1)` (line 7755). `--demand` parses through `_parse_demand` (line 1371).
- The row seals `needs_gpu=bool(demand.get("gpu"))` (in `tools/fleet/pbrun.py`, line 8415; stored in `src/prismabuild/pool.py`, line 7459).
- The claim gate `PoolQueue._placement_matches` (in `src/prismabuild/pool.py`, line 8087) refuses a GPU-demanded row to a host without a GPU (lines 8090-8091). A device-free row carries `needs_gpu=False`, so any host claims it. That is the CPU route.
- Device masking backs it: a demand without `gpu` receives `CUDA_VISIBLE_DEVICES=""` (in `tools/fleet/pbrun.py`, line 8001), so a CPU slot cannot touch a device.

k3 change: the device-free module submits without `gpu` demand (it drops `--gpu`). No routing code changes.

## Rejected alternatives

- Sealed `CUDA_VISIBLE_DEVICES` and sealed `demand.gpu`: they state submit-time intent, not executed-on fact. `pbrun.py` masks the variable for CPU slots (lines 7992-8001). The D38 design reads device hiding from the sealed environment only (in `docs/design.md`, lines 17515-17519).
- Broker and cgroup telemetry (`src/prismabuild/resource_scope.py`, lines 406-410; `PoolQueue._resource_profile` in `src/prismabuild/pool.py`, line 28697): they carry CPU, memory, and I/O totals. They carry no per-action device counter.
- Host GPU power (`src/prismabuild/box_capacity.py`, lines 376-397; `resource_profile_summary` in `src/prismabuild/pool.py`, line 3814): it is host-level. The placement proxy reads 1.0 on a throttled idle device, so it cannot tell zero use from idle.
- Per-process `nvidia-smi`: it reads null on GB10 (`src/prismabuild/slurm_lane.py`, lines 1626-1633). The node-side power seam returns `None` today (line 1712).
- The canary stamp alone: it covers only payloads that print it. Real GPU runs print none, so a stamp-only refusal would never fire for them.

## New record field

Yes, k3 adds one: the worker-observed device count on the attempt detail. The writer is the pool worker `PoolQueue._execute_in_checkout` (in `src/prismabuild/pool.py`, line 28820), into the outcome dict (lines 29559-29576). Neither the broker nor the payload program can cover every run; only the worker launches every run in the sealed environment. The CAS-bound artifact bytes stay unchanged.

## Final file list for k3

Copy this list into the k3 PR:

- `tools/fleet/pbrun.py` (refusal in `await_outcome`)
- `src/prismabuild/pool.py` (writer in `_execute_in_checkout`)
- `tests/test_pbrun_bounded_attachment.py` (existing closure tests; k3 adds the refusal test beside them)
- `docs/design.md`
- This note: `docs/gpu_closure_device_signal_2026-10-10.md` (reference; already landed)
