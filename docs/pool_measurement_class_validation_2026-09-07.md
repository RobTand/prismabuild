# Pool measurement class placement — 2026-09-07

Issue #366 adds explicit `--measurement --host-class CLASS` placement on the
pull queue. Ordinary measurements retain the submitting-host pin. The opt-in
keeps platform-keyed numerics, seals class placement and the actual platform,
ABI, shell executable, driver and GPU models/counts, and records the selected
host and GPU UUID in the receipt. Both arms of a paired experiment remain one
admitted action. CPU near-idle admission and CPU/GPU isolation are unchanged.

This is a placement capability, not a measured throughput improvement. Inner
container/Python environments remain caller-declared experiment dependencies;
the shell's attestation does not prove their contents.

## Admitted regression and compile evidence

All runs used published PrismaBuild, portable placement, priority -10, the
CPU test environment `/home/rob/venvs/pb-cpu/bin/python`, disabled GPU visibility,
and native OMP/MKL/OpenBLAS threads bounded to one. Actual terminal status,
cleanup, logs, canonical claims and receipts, result payload hashes/lengths,
and immutable source bundles were checked.

| Run | Action | Result |
| --- | --- | --- |
| Red, original implementation | `408ab33d0f657ea68aa157f3198fb72742391b92f20b42ebebb78f3cfb43263b` | Exit 1; exactly 2 failures at the existing pool host-class refusal, 2.92 s; dl380g10, CPU2/memory3 GiB |
| Initial integrated check | `efb892ce74c09f3da8c630d7b87c79730787b39007c06ebe65d8368d4b48e511` | Exit 0; 230 passed, 9.12 s; dl380g10, CPU4/memory4 GiB |
| Final compile and regression check | `65d58a90083fe11da1eb2d4d11a2ee5b774a31999b4ec88d7f55f944ab8c1cab` | Exit 0; 525 passed, no skips, 10.23 s; dl380g10, CPU8/memory8 GiB |

The final action compiled `core.py`, `pbrun.py`, `pbcampaign.py` and the new
measurement test module before running eight pytest workers across the new
class tests, existing host-class tests, core, campaign, placement, adaptive CPU,
adaptive GPU, CLI-help and design line-reference tests. Its receipt is
`31f93ceeda7c673c9145418ee386f416226af24425de8be5fa21d641d28f95a7`.
All nine changed source/test/contract files are byte-identical to its imported
snapshot; this dated evidence document was written afterward.

The regression fixtures simulate two matching GB10 workers with different UUIDs
and test class-constrained claims, one-process paired execution, producer
attestation, cache reuse, wrong ABI/architecture/model/driver/executable refusal,
missing identity, and receipt re-derivation. These are CPU contract tests, not
deployed GPU qualification or a measured cross-host numerical comparison.

Detailed evidence and exact commands:
`/home/rob/tmp/pb366-evidence/receipts.json`. The same directory retains actual
logs, `verify_receipts.py` and imported immutable bundles in `receipt-source.git`.

## Read-only host identity inspection

Both hosts answered direct `uname`, `getconf GNU_LIBC_VERSION`, shell SHA-256,
and `nvidia-smi --query-gpu=compute_cap,driver_version,name,uuid` inspection:

| Fact | Sparky | Sparklina |
| --- | --- | --- |
| OS / architecture | Linux / aarch64 | Linux / aarch64 |
| libc | glibc 2.39 | glibc 2.39 |
| `/bin/bash` SHA-256 | `af955ef55333c8fc9c5aa50df91ad1a629d9a79a9afa125cd5e9629585f78015` | Same |
| GPU / compute capability | NVIDIA GB10 / 12.1 | NVIDIA GB10 / 12.1 |
| NVIDIA driver | 595.84 | 595.84 |
| Physical UUID | `GPU-e76c7efc-c157-b1f4-1348-83e4eb5092f4` | `GPU-b1eceeea-fec7-371e-2cf3-cd10f2e7b705` |

These match the new compatibility fields while retaining different physical
devices as provenance. They establish neither idle capacity nor deployed
admission. Runtime publication and the first real class-placed paired job must
still be verified through its actual selected-host/UUID receipt and isolation
evidence. Existing active measurements must not be restarted for publication.
