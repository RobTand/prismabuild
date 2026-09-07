# Late-finisher scope cleanup — 2026-09-07

Issue [#370](https://github.com/RobTand/prismabuild/issues/370): a superseded worker could finish after a replacement claim and archive its result without releasing its own broker scope. The fix durably records the old attempt before cleanup, stops/releases only its exact scope nonce, and leaves failures for its host's normal reaper. It preserves the successor's claim, lease, resource reservation, terminal result, telemetry, and termination evidence. Pending cleanup blocks later admission until cleanup proof exists; existing immutable attempt records remain unchanged.

The nine new regression cases cover successful cleanup, stop/release refusal and restart recovery, conflicting live scope nonce, existing immutable history, a crash before the broker operation, foreign-host recovery refusal, admission deferral, interrupted scope creation, and a repeated late finish after the successor concludes. These use private queues and the existing fake broker fixture. The design contract is updated in `docs/design.md`.

## Validation

Before the production edit, the first two regression cases failed as expected: PB action `5b70612c405c8c55220ec9adc7b4180dc48dc19fd6ea64bc98e49acd59748aef`, exit 1, **2 failed in 3.83s**, cleanup complete. The old implementation never stopped/released the superseded scope and retained no retry authority. This ran on Sparky, CPU2/memory2 GiB, Python 3.12.3; its absolute interpreter path caused an implicit local pin. Final targets explicitly selected the compatible GB10 class instead.

Final targeted suite: **51 passed**, Sparky/GB10 ARM64, Python 3.12.3, CPU4/memory4 GiB, pytest 9.1.1 and xdist 3.8.0. Full suite: **3,203 passed, 3 skipped, 4 subtests passed**, Python 3.14.4 on DL380/x86, the same pytest/xdist versions. The supported PB fanout covered all 240 distinct repository test files exactly once in eight shards, each CPU4/memory4 GiB: aggregate CPU32/memory32 GiB. All eight admitted allocations used distinct preferred physical cores with no fallback allocation. Native threads were bounded to one. All actions were CPU-only.

The three skips were optional Dagster unavailable and the two opt-in SLURM container smokes disabled; there were 219 Python fork deprecation warnings. The suite does not certify these optional integrations. No performance claim is made: admission, timestamps and per-scope CPU/memory telemetry establish actual execution, but no contemporaneous host utilization profile was captured for this correctness run.

| Run | PB action key | Actual stdout result |
| --- | --- | --- |
| full-shard-0 | `2d21f1a413cc2591e00cd55fb185c5fab235bb4eb2552a4bb2989104191815af` | 519 passed, 58 warnings in 32.62s |
| full-shard-1 | `6d7802984c1a58207320bda52dac922edc2749029b0b89811f530fc342e8713c` | 412 passed, 10 warnings in 14.88s |
| full-shard-2 | `b555dad1d09b6955427d6ea27973d75d4e28769a1b547e66f0d5c5c9839b0310` | 551 passed, 1 skipped, 24 warnings in 17.87s |
| full-shard-3 | `3db560a3f9715a4949aafec3b6326dc7fdd4b0ef48415b38cde69f110d1d27a8` | 287 passed, 9 warnings in 12.63s |
| full-shard-4 | `c01b6e489baf58726759ae447db8a780553d1a5fffd5fe5fc9279056e77f3ba3` | 298 passed, 5 warnings in 13.88s |
| full-shard-5 | `05fd8fffbae63082285fbd9a3f3e002746935994ff7cb3b115f6c824d64b81af` | 449 passed, 8 warnings in 16.53s |
| full-shard-6 | `dd17046bc6fc34dd37c2e3521e08e6f3845bb310cf21273bea19e704fef65e3d` | 269 passed, 1 skipped, 102 warnings, 4 subtests passed in 14.88s |
| full-shard-7 | `4699645b88026b2360d295ca3fa36c5321c2d17a6ab84fed85616cdcd6381a22` | 418 passed, 1 skipped, 3 warnings in 16.09s |
| targeted | `1a30441fee6400e52e5fa77093033626e2612d68fb1c358d3de52c996f5bf986` | 51 passed in 4.21s |
| compile | `20b621e602e0f1eb0ef4dd56b93609466df47e9c1a3b333a5590e2353447052c` | compile exit 0 |

Every green action's terminal exit and cleanup flag, canonical CAS receipt hash, actual result payload size/hash, and actual Git source bundle hash were independently read and checked. Each bundle was imported and its `src/prismabuild/pool.py`, `tests/test_pool_late_scope.py`, and `docs/design.md` compared byte-for-byte with the final worktree. The red terminal, stdout and source bundle hash were checked; failed actions have no success CAS receipt. The [machine-readable audit](evidence/issue370/receipt-source-audit.json) records the full keys, receipt paths, payload hashes, source commits/member hashes, admitted allocations and telemetry. Raw stdout/stderr and the audit script are retained under `/home/rob/tmp/pb370-evidence/` and `/home/rob/tmp/pb370-audit.py`; the fanout manifest is `/home/rob/tmp/pb370-full-suite.json`.

Commands (coordinator submitted; admitted fleet workers executed):

```sh
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --cwd /home/rob/tmp/pb370-late-scope --tag gb10 --cpus 4 \
  --demand mem_gb=4 --priority -10 --timeout-s 300 --detach \
  --env PYTHONPATH=src --env OMP_NUM_THREADS=1 \
  --env MKL_NUM_THREADS=1 --env OPENBLAS_NUM_THREADS=1 -- \
  /home/rob/venvs/pb-cpu/bin/python -m pytest -q -n 4 \
  tests/test_pool_late_scope.py tests/test_pool_late_finisher.py \
  tests/test_pool_resource_scope.py tests/test_pool_tombstone_capacity.py \
  tests/test_pool_finish_tombstone.py

python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py \
  --checkout /home/rob/tmp/pb370-late-scope \
  --python /home/rob/venvs/pb-cpu/bin/python --tag x86 \
  --shards 8 --workers-per-shard 4 --threads-per-shard 1 --mem-gb 4 \
  --timeout-s 600 --priority -10 \
  --json /home/rob/tmp/pb370-full-suite.json tests
```

A separate admitted CPU1/memory1 GiB x86 action ran `python -m py_compile src/prismabuild/pool.py tests/test_pool_late_scope.py` successfully. Adding this validation report and its evidence after the checks changes no tested code or design contract. This is a deterministic private-queue regression suite, not live destructive recovery. Runtime rollout must upgrade all claimants before relying on the new `.late-finish` admission barrier.
