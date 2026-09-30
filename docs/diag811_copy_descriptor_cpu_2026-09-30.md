# Experimental checkout-copy descriptor retention: CPU evidence

This is a scoped repair for [PB #811](https://github.com/RobTand/prismabuild/issues/811), not production cache qualification or a performance result.

## Behavior

`tools/maintenance/diag_811_e1_checkout_cache.py::copy_or_reflink` previously closed its source after an unsupported reflink, then reopened the original pathname for byte-copy fallback. Replacing that pathname selected different bytes; unlinking it aborted the copy. The existing private-destination digest check already refused changed bytes, so this finding does not establish wrong-artifact acceptance.

The helper now holds the selected source open through fallback. On Linux, `shutil.copyfile` opens `/proc/self/fd/<held descriptor>` instead of the original pathname. This retains the selected physical object and the standard library's fast-copy path. Both descriptors close on return; the private-destination digest checks are unchanged. This is not atomic acquisition against the previously verified cache identity.

## RED and GREEN

Both runs used the published `pbtest.py`, one CPU-only shard, two pytest workers with `--dist worksteal`, one native thread per worker, two reserved CPUs, 2 GiB memory, priority -10, and explicit 600 s execution/wait bounds. Placement used the coordinator's explicit PB-self-test `gb10`/`pb-cpu` route. `WINDOW_ACTIVE` was absent before each client started. PB placed both actions on Sparky with `CUDA_VISIBLE_DEVICES=''`.

| Run | Action | Terminal | Population |
| --- | --- | --- | --- |
| RED, unchanged helper plus regression | `f8d34093130cb065db375551993b608f9b6f788899034f4ccfae5d3343cad9a9` | failed, exit 1, one attempt | 2 failed, 2 passed, 0 skipped; 4 collected/ran/outcomes |
| GREEN, repaired helper and all four adjacent harness files | `d84185d95f05deac99425fd185e15135c46235f8ce8f9d3f7d80fb03cfb98c3d` | done, exit 0, one attempt | 24 passed, 0 failed/skipped; 24 collected/ran/outcomes |

Neither run had missing files, uncollected cases, duplicated collection, or outcome-reconciliation gaps. The two RED failures were:

- `test_copy_fallback_retains_the_opened_object[replace]`: `AssertionError: fallback reopened a different object`.
- `test_copy_fallback_retains_the_opened_object[unlink]`: `FileNotFoundError: [Errno 2] No such file or directory`.

The ordinary-copy and simulated-reflink controls passed before the fix. The added cases inject ioctl outcomes while exercising real small files, pathname replacement/unlink, byte-copy fallback, and descriptor closure. They do not qualify filesystem reflink support, real Git packs, a production lease, or a device population.

GREEN included:

- `tests/test_diag811_copy_descriptor.py`
- `tests/test_diag811_entry_publication.py`
- `tests/test_diag811_experiment_boundaries.py`
- `tests/test_diag811_generation_guard.py`

These imports compile the changed helper. `git diff --check` passed. The primary LSP probe reported no findings on the changed Python files; auxiliary pattern/typo rules reported 30 findings in the harness, including propagated I/O calls. This is not a blanket lint-clean claim.

## Receipt and logs

GREEN receipt SHA-256: `8f77597e46062db159bba93bad4e06bcd30707f563733bda9aff7c72cbddcf51`.

GREEN result: 6,575 bytes, SHA-256 `7d8233a3cfc068995506b3b0aec8f0f3631021840365116cec1a509f502b8eee`.

Worker-emitted local-result claim: `e62d5088d56be8a598f6c6b8c79002870c4b13e1c5e1c8dd1a1e8f575c70a848`. All nine `pb_verify_claim(hash_payload=true)` checks passed. Full worker attestation was not independently verified; the verifier explicitly leaves that check to `core.PrismaBuildCAS.lookup` with the full action manifest.

The failed RED action has no successful CAS receipt or local-result claim. Its terminal index, one-attempt outcome, log, and reconciled population establish RED, not a successful result.

Coordinator logs and population reports:

- `/home/rob/tmp/claude-campaign-20260926/tmp/p2p3/prismabuild/811-descriptor-red-ts.log`
- `/home/rob/tmp/claude-campaign-20260926/tmp/p2p3/prismabuild/811-descriptor-red-ts.json` — SHA-256 `c451738bf41b5cbb8efcae30fba18667ef2100970182435d5490cc1e1c2e57d3`
- `/home/rob/tmp/claude-campaign-20260926/tmp/p2p3/prismabuild/811-descriptor-green-ts.log`
- `/home/rob/tmp/claude-campaign-20260926/tmp/p2p3/prismabuild/811-descriptor-green-ts.json` — SHA-256 `030cfafd6521bde5fc1780a2ec336f24b6ac86ebb4808825664cd786d66e4db2`

## Remaining acceptance

The scoped merge record is `docs/diag811_copy_descriptor_acceptance_2026-09-30.json`. It records an experimental subset of RNG-02; the global requirement ledger's axes remain unchanged.

This repair does not implement production publication-to-acquire fencing, reader references, async/mmap lifetimes, retirement/charge ordering, crash recovery, or cache integration. It changes no runtime generation, default, sealed input, or certification. `/proc/self/fd` availability is a Linux-helper dependency; no other platform is qualified. No power-loss durability, performance delta, or speedup is established. Production integration and matched deployed claim-path profiling remain owed, and #811 remains open.
