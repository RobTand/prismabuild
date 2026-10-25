# Record-compat controller validation for #2253

The controller population moves from one file to nineteen family files. The 237 native cases, inherited RecordContract assertions, standalone test bodies, goldens and field sets remain unchanged. No test is deleted, skipped, loosened or replaced with a timing assertion.

Verified generation closures now share one lifetime per pytest configuration. Exact source bytes key the definition index. Each replay retains its own process, store, bundle cache and source/code attestation. Path reads remain fresh so changed source selects a new index.

## Qualification and scope

Validation uses the existing namespace-x86 lane: `/home/rob/venvs/pb-cpu/bin/python3.14`, the pinned archival executable digest `52e0a13e60a981d8c4b6478be2ba5176f69da07948a056bf49cf6f077e30cb41`, `TMPDIR=/home/rob/pbt17`, native thread limits of one, one CPU per pytest worker and four GiB per action. Full controller execution is x86-only under its existing `archival-replay` capability. The portable setup tests also run on ARM with Python 3.12.

`TMPDIR=/tmp` reproduces the separate #2178 archival qualification defect. The existing documented lane passes its controls. This PR does not change that qualification code. Release gate selectors that enumerate only the old controller filename must select `tests/test_record_compatibility_controller*.py` before release.

## Evidence

The fresh unsplit baseline passed all 237 cases in **1,727.21 seconds**, or 28 minutes 47 seconds. Action `e97cc5afc960a09214068d5f3f9c676401c264ffeee6819e3a949d95617dfba5` ran unchanged main.

All five split runs passed 237 cases with zero failures or skips. Their 95 distinct action keys have 95 distinct native execution nonces.

| Run | Workers per shard | Cases passed | Slowest shard, pytest seconds | Host CPU busy mean for slowest shard |
|---|---:|---:|---:|---:|
| 1 | 1 | 237 | 213.70 | 60.9% |
| 2 | 1 | 237 | 202.07 | 55.1% |
| 3 | 1 | 237 | 200.05 | 45.6% |
| 4 | 2 | 237 | 112.60 | 51.2% |
| 5 | 2 | 237 | 140.73 | 56.4% |


- Recovered source: PR #2269, HEAD `0dfaf0a38ed1568fe38611904f6b4762d74b0151`, based on main `9dce28460e76d4115c925bac8efc4f397160f9cf`.
- Native population check against the fresh baseline: all five runs retain 237 cases, with empty normalized node diffs and 153 protected files byte-identical. Action `f286af30bc657ed2e874a79fb516fe05b8e080c82b958727e1c6999461c2dc4b`.
- Composed session: 54 passed in 225.01 seconds. This combines two controller families, negative compatibility controls, field-set inventory checks and namespace preflight tests in one pytest configuration. Action `0e717e7c94377c83741c456cf8924099b52922c7dcceb9c3998cffafcb9778d3`.
- Portable setup: five executed runs on each of x86 and ARM, including two runs with two xdist workers on each architecture. Two tests pass in every execution, without sleeps or runtime thresholds.
- Five negative controls fail at the intended behavior: stale path cache, repeated source extraction, missing cleanup, missing definitions, and disabled indexing.

The timings below describe executed tests under fleet load. They are not an isolated causal speedup measurement. PB resource profiles retain host busy/pressure observations. Another operator published a runtime during validation; action receipts retain each exact runtime generation. This task performed no runtime publication.

## Profile

The bounded stage probe `3468e5b6ea60e2772aa2ec986e871de05954c7c8f35463ebf1942a1139d16955` observes 1.32 seconds for generation materialization. For two instrumented current writes, compilation takes 0.86–1.20 seconds and profile-hook work takes 1.14–1.63 seconds. Instrumentation adds overhead. One historical shape probe reports `adapter reached no production imports`; it is excluded from successful timing/proof claims. The current-write probes show why replay attestation remains substantial work after shared setup is cached.

## Claim preservation

The three inherited RecordContract tests retain their original bodies for every family: exact production writes and supported consumers, unknown-field policy, and invalid-record behavior at both consumer boundaries. The table records each standalone test's move. No standalone test is rewritten.
| Existing test | Claim | Proof after the split |
|---|---|---|
| `test_a_missing_archival_executable_does_not_fall_back_to_the_gate` | Missing pinned executable refuses before writer creation | Same test and assertions in `test_record_compatibility_controller_archival.py` |
| `test_archival_executable_identity_mismatch_stops_before_the_writer` | Executable digest mismatch refuses before writer creation | Same test and assertions in `test_record_compatibility_controller_archival.py` |
| `test_archival_writer_uses_the_selected_environment_after_ambient_change` | Python and native ambient mutations do not change the selected child environment | Same test and assertions in `test_record_compatibility_controller_archival.py` |
| `test_archival_writer_refuses_a_changed_child_environment` | Child environment mismatch refuses before writer creation | Same test and assertions in `test_record_compatibility_controller_archival.py` |
| `test_malformed_archival_stdout_refuses_a_complete_zero_exit_child` | Malformed archival runtime proof refuses even when process exits zero | Same test and assertions in `test_record_compatibility_controller_archival.py` |
| `test_archival_runtime_cannot_replace_the_gate_writer_or_reader` | Archival runtime may only replay manifested historical writes | Same test and assertions in `test_record_compatibility_controller_archival.py` |
| `test_historical_shape_write_keeps_its_gate_runtime_observation` | Gate runtime observation retains actual historical shape writer execution | Same test and assertions in `test_record_compatibility_controller_archival.py` |
| `test_current_all_members_export_reaches_the_current_writer_and_reader` | Current all-members export reaches current production writer and consumer | Same test and assertions in `test_record_compatibility_controller_upgrade.py` |
| `test_current_all_members_export_reaches_each_retained_exact_whitelist` | Retained exact whitelist refuses the expanded current export | Same test and assertions in `test_record_compatibility_controller_upgrade.py` |
| `test_current_all_members_source_refuses_the_frozen_designated_window` | Current expanded source refuses the frozen rollback window | Same test and assertions in `test_record_compatibility_controller_upgrade.py` |

The [action manifest](record_controller_2253_actions.tsv) records every controller execution and the supporting controls with receipt digests.

## Per-file pytest durations

| Family | Run 1 | Run 2 | Run 3 | Run 4, xdist | Run 5, xdist |
|---|---:|---:|---:|---:|---:|
| runtime | 72.65 | 72.56 | 63.63 | 38.50 | 39.62 |
| archival | 29.39 | 28.99 | 24.84 | 22.88 | 26.23 |
| broker | 46.55 | 54.47 | 59.31 | 23.93 | 33.58 |
| config | 38.05 | 39.85 | 33.49 | 28.39 | 29.09 |
| edges | 164.99 | 158.50 | 165.53 | 83.61 | 81.85 |
| floor | 196.51 | 196.75 | 200.05 | 101.61 | 140.73 |
| gang | 156.89 | 146.18 | 148.18 | 77.32 | 76.97 |
| inputs | 67.58 | 69.04 | 60.10 | 36.45 | 39.02 |
| markers | 26.57 | 27.66 | 24.82 | 15.36 | 14.09 |
| outcomes | 213.70 | 202.07 | 180.33 | 112.60 | 132.37 |
| preflight | 45.43 | 44.49 | 56.76 | 28.14 | 29.04 |
| prelaunch | 127.58 | 122.38 | 145.70 | 104.69 | 76.65 |
| prelaunch_release | 74.59 | 76.45 | 76.20 | 41.52 | 41.41 |
| prelaunch_turns | 175.45 | 171.22 | 172.91 | 85.88 | 90.04 |
| reclaim | 121.41 | 117.16 | 119.15 | 61.69 | 64.63 |
| rollout | 18.85 | 17.92 | 19.06 | 12.49 | 13.25 |
| slurm | 168.15 | 163.73 | 151.64 | 81.01 | 90.69 |
| state | 85.82 | 84.00 | 78.87 | 43.25 | 50.00 |
| upgrade | 40.46 | 37.08 | 33.71 | 20.38 | 21.86 |

## Reproduce through PrismaBuild

Use the published client. The serial command below is the measured lane; use `--workers-per-shard 2 --cpus-per-shard 2` for xdist. Every repeat seals a distinct `PRI20_VALIDATION_REPEAT` value. PB owns file distribution. The default fanout caps this population at one action per file.

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py \
  --checkout CHECKOUT --python /home/rob/venvs/pb-cpu/bin/python3.14 \
  --tmpdir /home/rob/pbt17 --workers-per-shard 1 --threads-per-shard 1 \
  --cpus-per-shard 1 --mem-gb 4 --timeout-s 600 --test-timeout-s 180 \
  --wait-s 5400 --env PRI20_VALIDATION_REPEAT=1 --json OUTSIDE_CHECKOUT/after-1.json \
  tests/test_record_compatibility_controller*.py
```

The baseline command uses main, the single original controller file, `--timeout-s 3600`, the same one-CPU/four-GiB demand and native thread limit, and no repeat environment value.

`tests/compat/support/check_controller_population.py` compares native executed collection records, original standalone bodies, inherited assertions, and protected bytes. Run it inside an admitted action with report excerpts transported in the checkout and a full base commit, since worker bundles need not expose `origin/main`.

The attached review bundle contains compact native records, all action keys and receipt digests, load observations, negative-control failure excerpts, and the claim map. Full action stdout remains in CAS. All controller receipts and scope cleanup records were checked; representative payloads were also hashed through `pb_verify_claim`. That tool's claim/payload checks do not independently attest an entire action manifest.

## Review and release

The branch is rebased on main. The Principal Engineer owns independent standards/spec review, merge and release. The existing `Fixes #2253` link closes the issue only when the reviewed PR merges. Source changes do not publish the fleet runtime.

Fix Root Causes and Laziness Protocol keep the change to shared immutable setup and file-level sharding. Build the Lever provides the population checker and stage probe. Prove It Works requires executed fleet outcomes and receipt checks.
