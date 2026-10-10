# GPU closure device-use signal: decision note (prismabuild#1743)

Serves parent prismabuild#1740 criterion C2. No product code changes. All paths and lines cite `origin/main` at `561e3177be3c8e6f3e21e2f7ffa5abddf0c91663`. Revision 4 defines what the launcher probe counts (see "What the probe counts"), fixes the pool spawn cite, and names the closure's `needs_gpu` source, its exit code, and the cache-hit path.

## Signal source (one)

The one signal source is a launcher-observed device count on the attempt record. The count means: devices visible in the sealed launch environment. It does not mean devices used. The launcher counts them at execution time (see "What the probe counts"). The closure reads the count from the adopted attempt. One field answers zero versus one or more devices for every GPU run.

A new record field is needed. No existing per-action field carries executed-on device fact. Sealed `demand.gpu` and sealed `CUDA_VISIBLE_DEVICES` state submit-time intent, not fact. Broker and power telemetry are host-level. The canary stamp covers canary runs only (see below).

## Writer that emits the signal for every GPU run

Correction: the pool worker is the wrong writer. `PoolQueue._execute_in_checkout` (in `src/prismabuild/pool.py`, line 28820) runs in the pool process. It spawns the launcher as a child with `subprocess.Popen` (lines 28976-28990; lines 28855-28862 only build `argv`) and reads the launcher's stdout afterwards (lines 29559-29576). A probe there sees host devices, not task devices.

The sealed environment is built in the launcher. `run_local_action` (in `src/prismabuild/core.py`, line 7965) builds `launch_environment` (lines 8066-8071), applies the profile wrapper (lines 8072-8078), and execs the sealed argv in it (lines 8079-8092). The probe must run there, after line 8078, so it sees the devices that the sealed launch environment makes visible. For a container payload that is not the same set as the container sees (see "What the probe counts"). `docs/design.md` (lines 17515-17518) rules anything else out: the check reads the sealed environment, and host accelerator inventory is not task visibility.

The lift pattern already exists. The launcher result object carries its own `profile` key (in `src/prismabuild/core.py`, lines 8260-8264). `core.main` prints the result object as the last stdout line (line 8987). The pool lifts that key out of the launcher stdout with `profile_from_launcher_stdout` (in `src/prismabuild/pool.py`, line 29703) and files it into the outcome (lines 29573-29575). k3 adds the device count the same way: the launcher emits it in its result object, and the pool lifts it into the outcome beside `profile`.

Filing and reading path, already built:

- `archive_attempt` (in `src/prismabuild/pool.py`, line 25719) files attempt details first-writer-wins.
- `attempt_outcomes` (in `src/prismabuild/pool.py`, line 25997) reads the immutable linked attempts.
- `adopted_attempt_summary` (in `src/prismabuild/pool.py`, line 26371) selects the immutable winner.
- `outcome_summary` (in `tools/fleet/pbrun.py`, line 3453) reduces the ending to reported fields.

Fields the closure reads for each GPU-demanded run:

- New launcher field `devices` on the adopted attempt detail: the executed-on count.
- `detail.returncode` / `detail.action_returncode`: the exit-zero gate.
- Sealed `demand.gpu` / `needs_gpu`: the GPU-run gate. The closure takes it from the sealed action, `params.demand.gpu` (the submitter writes `needs_gpu` from it in `tools/fleet/pbrun.py`, line 8415; the pool item stores it in `src/prismabuild/pool.py`, line 7459). `await_outcome` has no such parameter today (signature at `tools/fleet/pbrun.py`, line 3792). k3 passes the value from the submit path, which holds `demand`, and reads the pool item's `needs_gpu` when only the key is known (the deferred-release call at line 8764 and `tools/fleet/qualify_claim_recovery.py`, line 321).

## What the probe counts

The count is the number of devices visible in the sealed launch environment. It is a visibility count, not a use count. It answers: can the payload process see a device? It does not answer: did the payload run a kernel?

The probe is one short child process that the launcher starts with `env=launch_environment`, the same dict as the payload (built at `src/prismabuild/core.py`, lines 8066-8071). The child calls the CUDA driver API through `ctypes`: `cuInit(0)`, then `cuDeviceGetCount`, on `libcuda.so.1`. It imports no `torch`. This call honours `CUDA_VISIBLE_DEVICES` the same way `torch.cuda.device_count()` does, so a CPU slot sealed with `CUDA_VISIBLE_DEVICES=""` (`tools/fleet/pbrun.py`, line 8001) counts zero.

Rejected inputs for this count: `nvidia-smi -L` and NVML (`nvmlDeviceGetCount`). Both ignore `CUDA_VISIBLE_DEVICES`, so they would count the host inventory. That is the host view that `docs/design.md` (lines 17515-17518) rules out.

If `libcuda.so.1` is missing, `cuInit` fails, or the child times out, the launcher writes `devices: null` and a short `devices_probe_error` string, not `0`. Only an integer `0` refuses.

Limit for container payloads. `tools/fleet/pbcanary_legs/leg2.py`, line 116, runs `docker run --rm --gpus all`. The container devices come from the docker flags and the NVIDIA container runtime, not from the launcher environment. So for a container payload the launcher count is the visibility of the launching environment, not of the container. The note makes no claim that it equals the container's own device count. Two effects follow:

- A GPU-demanded run whose sealed environment shows zero devices is a placement or masking fault, and the refusal is correct even for a container payload.
- A container payload that sees zero devices inside the container while the launcher sees one or more is not caught by this signal. The payload self-stamp (the canary pattern below) is the only evidence for that case. k3 states this gap in `docs/design.md` and does not claim container coverage.

## Canary stamp is the pattern, not the source

`tools/fleet/pbcanary_legs/leg2.py` proves the stamp shape, and only that:

- `_inner_script` (line 66) prints the stamp (lines 76-78), refuses zero devices (lines 79-81), then runs the kernel op (lines 82-84).
- `verify` (line 134) fails `DEVICES < 1` (lines 158-162) and fails a missing kernel line (lines 163-167).
- `DEMAND` (line 59) seals `gpu: 1`, so the stamp belongs to a GPU-demanded run.

Real GPU runs print no such stamp. A refusal that reads only the canary stamp would never fire for them. The launcher field covers every run because the launcher launches every run.

## Run records (zero versus one or more)

The payload below is an ordinary GPU-demanded torch job with no self-probe. It takes the CPU path when no device is visible and exits 0. It prints no stamp. `leg2.py` cannot produce the zero-device record: it exits 11 on zero devices (lines 79-81), so `DEVICES 0` with `returncode` 0 never comes from it.

Zero-device record (must refuse):

- `stdout`: application output only, no stamp.
- `detail.returncode`: `0`. Sealed `demand.gpu`: `1`. New launcher field `devices`: `0`.
- Read result: exit-zero GPU-demanded run with zero executed-on devices. Verdict: refuse.

One-or-more-device record (closes as before):

- `stdout`: application output only, no stamp.
- `detail.returncode`: `0`. Sealed `demand.gpu`: `1`. New launcher field `devices`: `1`.
- Read result: exit-zero GPU-demanded run with one executed-on device. Verdict: close with exit 0.

Test note: `tests/test_pbcanary_legs12.py` builds stamp shapes in `_leg2_artifact` (lines 132-140) and `test_leg2_verify_zero_devices_fails_cuda_visibility` checks `verify`, not `await_outcome`. These tests evidence stamp semantics only. The refusal test belongs to k3 and targets `await_outcome`.

## Closure site that must refuse

File: `tools/fleet/pbrun.py`. Function: `await_outcome` (line 3792). It blocks until the pool action lands, renders the summary, prints the headline (line 4042), and returns 0 for `executed` / `cache_hit` / `returncode` 0 (lines 4043-4049). k3 adds the refusal there: a GPU-demanded exit-zero run whose launcher `devices` field reads zero returns nonzero instead of 0 and names the reason. The exit code is a new named constant beside `RECORD_WRITE_FAILED_EXIT` (`tools/fleet/pbrun.py`, line 161), added to `RESERVED_EXITS` (line 174) so that `reported_exit` (line 4153) cannot let an action's own status impersonate it. k3 picks the number; it must not be 0, 2, 74, 75 or 143.

Cache-hit path. `await_outcome` returns 0 for `cache_hit` at lines 4043-4044, before it reads `detail.returncode` at line 4045. A resubmit of a zero-device run would then close with 0 and bypass the refusal. k3 must run the device check before that early return, on the adopted attempt of the cached result, or the cache must not serve a run whose record has `devices: 0`. A test must cover the resubmit. k3 states the choice in `design`. Field reduction stays in `outcome_summary` (line 3453). A pre-field record with no `devices` field closes as before; absence is a recorded gap, not a refusal.

## CPU routing site for a device-free module (corrected)

Correction: dropping `--gpu` does not route to CPU from a Spark host. `placement_contract` (in `tools/fleet/pbrun.py`, line 1747) derives the `gb10` tag when the hostname sits in the `gb10` roster, with no `needs_gpu` condition on that branch (lines 1778-1782). The claim gate `PoolQueue._placement_matches` (in `src/prismabuild/pool.py`, line 8087) then requires every listed tag on the claiming host (lines 8092-8095). A tag-only `gb10` row still admits GPU-class hosts only. The `needs_gpu` arm (lines 8090-8091) is not the whole gate.

The real routes run through the same two functions:

- Explicit `--tag x86` (flag in `tools/fleet/pbrun.py`, lines 7226-7227) outranks every derived pin: `placement_contract` returns the explicit list verbatim (lines 1756-1758). The row then requires the `x86` tag, so a CPU-class host claims it.
- `--anywhere` (flag in `tools/fleet/pbrun.py`, lines 7286-7288) returns no tags (lines 1761-1762). The row admits any host, and the `needs_gpu=False` arm lets a CPU host claim it.
- A third route edits the `hostname in aliases` condition (in `tools/fleet/pbrun.py`, line 1779) so a device-free submit skips the `gb10` tag. That edit changes placement for every Spark submit, so a person must decide it. k3 makes no such edit.

k3 decision: the device-free module submits with explicit `--tag x86` and no `gpu` demand. `--anywhere` asserts identical dependencies on every worker, which a torch module cannot assert. `--tag x86` is the narrow true claim. Demand sealing backs it: the row stores `needs_gpu=False` (in `tools/fleet/pbrun.py`, line 8415; stored in `src/prismabuild/pool.py`, line 7459), and a demand without `gpu` receives `CUDA_VISIBLE_DEVICES=""` (in `tools/fleet/pbrun.py`, line 8001), so a CPU slot cannot touch a device.

## Rejected alternatives

- Sealed `CUDA_VISIBLE_DEVICES` and sealed `demand.gpu`: they state submit-time intent, not executed-on fact. `pbrun.py` masks the variable for CPU slots (lines 7992-8001). The D38 design reads device hiding from the sealed environment only (in `docs/design.md`, lines 17515-17519).
- A pool-process probe beside `returncode` in `_execute_in_checkout`: it runs in the pool process, outside the sealed environment. It reads host inventory, which `docs/design.md` (lines 17515-17518) excludes from task visibility.
- Broker and cgroup telemetry (`src/prismabuild/resource_scope.py`, `read_cgroup` at lines 60-69 returns `cpu_seconds`, `memory_current_bytes` and `memory_peak_bytes`; `PoolQueue._resource_profile` in `src/prismabuild/pool.py`, line 28697): they carry CPU, memory, and I/O totals. They carry no per-action device counter.
- Host GPU power (`src/prismabuild/box_capacity.py`, lines 376-397; `resource_profile_summary` in `src/prismabuild/pool.py`, line 3814): it is host-level. The placement proxy reads 1.0 on a throttled idle device, so it cannot tell zero use from idle.
- Per-process `nvidia-smi`: it reads null on GB10 (`src/prismabuild/slurm_lane.py`, lines 1626-1633). The node-side power seam returns `None` today (line 1712).
- The canary stamp alone: it covers only payloads that print it. Real GPU runs print none, so a stamp-only refusal would never fire for them.

## New record field

Yes, k3 adds one: the launcher-observed device count on the attempt detail. The writer is the launcher `run_local_action` (in `src/prismabuild/core.py`, line 7965): it probes inside the sealed environment built at lines 8066-8078, emits the count in its result object beside `profile` (lines 8260-8264), and prints it as the last stdout line via `core.main` (line 8987). The pool lifts it with the `profile_from_launcher_stdout` pattern (in `src/prismabuild/pool.py`, line 29703, applied at lines 29573-29575). Neither the broker nor the payload program can cover every run; only the launcher runs every run inside the sealed environment. The CAS-bound artifact bytes stay unchanged.

## Final file list for k3

Copy this list into the k3 PR:

- `tools/fleet/pbrun.py` (refusal in `await_outcome`; submitter route `--tag x86`)
- `src/prismabuild/core.py` (launcher probe in `run_local_action`; count in the result object)
- `src/prismabuild/pool.py` (lift into the outcome beside `profile`)
- `tests/test_pbrun_exit_vocabulary.py` (existing `await_outcome` tests; k3 adds the refusal test beside them)
- `docs/design.md`
- This note: `docs/gpu_closure_device_signal_2026-10-10.md` (reference; already landed)

List correction: `tests/test_pbrun_bounded_attachment.py` never calls `await_outcome` (verified by search: no match). `tests/test_pbrun_exit_vocabulary.py` calls it at lines 77, 91, 112, and 124. The earlier list named the wrong test.
