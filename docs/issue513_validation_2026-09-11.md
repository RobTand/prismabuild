# Issue 513: native Nsys and Docker

2026-09-11. This implements the early-refusal alternative in #513. Native Nsys cannot inject into a CUDA process launched by the Docker daemon. The new guard reaches the Docker shim through the action environment and refuses container execution before contacting the daemon. It does not implement transparent container instrumentation.

## Reproduction

Baseline `f0fe6bfc0667a1c38c73001e0a151364c697ab90`, using the published runtime `cb56f25d248f-1789164313-f65da1dcef64` (published code bytes compared with main). All execution used published `pbrun.py` or `pbtest.py`, priority -10, with one native thread per reserved CPU.

- CPU regression action `c7e4413b22ca3b5e8036a4645dd0d707eeb521c31d830ec9ca5288d71680c83d`: **15 failed, 1 passed** before the fix. Container commands reached the recording fake daemon; the Nsys environment guard was absent. Four CPUs/4 GiB, four pytest processes on Sparky. No real daemon was contacted by these tests.

- GPU baseline action `603086b030ff730b3f2ad841acabeeb4c24336228176450d90edb35d10b55312`: the reported producer image completed 64 fixed 512x512 CUDA matrix multiplies, checksum `23.791635513305664`, on NVIDIA GB10/Sparklina. Launcher and container both retained CPU `[5]`. Nsys 2025.3.2, `nsys:20`, produced a 117,322-byte report with `kernel_summary_absent`: no completed kernel rows. Backend and action exited 0. The completed arithmetic and absent kernel evidence reproduce the instrumentation gap; this is not a GPU performance measurement.

Baseline report: CAS blob `ef0456ae6e9297c3f4070ca6118da9a48a14c86e1f9755adb3270ffe51174764`. Result claim `c6c0b0bceb34ac77b93438bdedaf586d23a5e605278697b0b6a04665c4b38e79` resolved to a verified receipt/payload. Blob digest and byte count were checked.

## Validation

Nine affected profiling and Docker test files, distributed by PB into four shards on dl380g10: **201 passed, no skips or missing tooling**. One CPU/2 GiB per shard, one native thread, 240-second action deadline. The new file contributes 22 cases.

| Shard | Result | Action key |
|---|---|---|

| 0 | 53 passed in 11.70s | `c77266210b24e26eaba66fb3f8fff76f7277b87ce1d50e2989103a29f58fafb5` |

| 1 | 44 passed in 12.65s | `38764176a027663ae1e6820134c9199b4f8746bf173f59a957b0bb6a1d9af6bb` |

| 2 | 67 passed in 17.62s | `cfa569b623e2344793bebde75c94ee7ae3e901bad73e9ad3779c4a4f7b8c78dd` |

| 3 | 37 passed in 8.54s | `243c4aea89c98e08b9c028009b41dcc20c50a849d99d54d45492f11442181fb1` |


Each terminal record reports `executed`/0. The local-result claims resolve through `pb_verify_claim(hash_payload=true)` with complete reads and all performed checks passing; the payload contains the actual pytest summary. Scope peaks were 146–172 MB and process I/O had no unreadable members. This receipt check does not independently re-run full worker attestation.

GPU checks used either eligible GB10 worker (PB selected Sparklina), one CPU, 4 GiB aggregate memory, 2 GiB GPU subset, and a 120-second hard deadline. The image was inspected and restricted to the known local IDs, then launched by immutable ID. On Sparklina that ID is `sha256:9f9b9f05b17531399ba66dc6415b054cf5d68c82270626d0e9150e75c808435f`.

- Candidate guard action `c9823916e500e0689eef74b28a771afeb53707a4d0d1d29752d56e662a0aab9a`: under the real Nsys wrapper, the probe obtained the candidate `_ProfileSession.environment()` and selected the candidate Docker shim from the submitted snapshot. Image inspection succeeded; run refused with the remedy before the CUDA payload executed. Terminal worker rc 1, action rc 125, no success receipt. The negative report and `kernel_summary_absent` were retained and its blob hash verified. This is candidate qualification, not deployed adoption.

- Existing in-container PyTorch route, action `6f74d5bf7c742ba250dc0d1996c42ce01c32d1e9af707440295b98ec285107c1`: `--profile torch`, with the profile output directory mounted and `PRISMABUILD_PROFILE_TORCH_OUT` forwarded; the sealed copy of `tools/profile_torch.py` surrounded the same fixed work inside the container. The checksum remained `23.791635513305664` and both CPU masks were `[16]`. CAS blob `d99006cad1112e22d1786264dfb2c6009cd4b76d7a03f75df23be13c7623a764` is 13,495 bytes (263,084 decoded), with **65 completed kernel, 177 CUDA runtime and 64 CUDA driver events** (`ph=X`, positive duration). Receipt/payload and profile digest were checked. This qualifies the documented PyTorch remedy for this image, not arbitrary images or automatic Nsys instrumentation.

## Scope and artifacts

The guard applies to both Nsys modes, Docker global options, run/create/exec, and previously unsupported start/compose-start forms. Metadata inspection remains available. Existing CPU affinity, scope, summary timeout, trace budget, primary checkpoint and exit-status tests passed. The shim and environment are operational contracts, not a security boundary against a payload deliberately clearing variables or bypassing the shim.

Run artifacts and the bounded probe source are retained at `/home/rob/tmp/pb-513-maintenance/`. They include submissions, MCP action readbacks, verified claims, raw test output and profile counts. PB CAS snapshots bind the actual probe source. Existing user worktrees and active study requests were preserved.
