# gen12 full-suite gate: result and how the differences were explained

Candidate: head `34c2228cb2` (main `561e3177be` + the PR 1640 revert), gate tree `2a201c93b7`.
Command: `pbtest.py --checkout /home/rob/wt/pb-gate-gen12 --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --shards 20 tests`.

## Result (read from the result file before it was deleted)
20 shards. 13,507 passed, 13 failed, 13 failing IDs. Shards 16, 18 and 19 returned rc 1. No shard lacked a summary.

## Comparison
Baseline: train `pb-9-r1-full`, the same main tree: 13,643 collected, 4 failing IDs. Of my 13, **11 are not in that list**:
- 9 tests in `tests/test_filesystem_floor_1483.py`: `test_a_busy_floor_lock_is_waited_for_then_refused_as_a_shortage`, `test_a_root_on_another_filesystem_marks_the_binding_moved`, `test_a_stale_sample_refuses_and_a_skewed_clock_is_tolerated`, `test_cli_register_status_check_refresh`, `test_concurrent_acquire_release_refresh_across_two_ledgers_never_deadlocks`, `test_floor_section_runs_inside_a_held_ledger_lock_without_deadlock`, `test_register_refresh_admit_and_refuse`, `test_release_needs_no_floor_and_refresh_returns_the_headroom`, `test_used_paths_unbound_local_is_sampled_fresh_and_bound_is_published`
- `tests/test_pbstatus_pool.py::test_pool_status_reads_shared_admission_evidence_without_writing`
- `tests/test_an_owner_waiting_on_its_own_export_is_not_no_progress.py::test_an_owner_waiting_on_its_live_export_is_not_killed_no_progress`

Two IDs failed in pb-9 and not in mine: `test_ten_thousand_polls_against_a_tight_replace_loop_reject_nothing` and `test_tier_advance_reports_the_floor_as_tier_short`.

## Explanation
1. **The pbstatus test and the owner-export test** pass when the three files are rerun alone on the candidate (21 and 15 passed; actions `6103b0afcc19` and `891850a01206`). They failed under load. `gate-evidence/gen12-rerun3.*`.
2. **The nine floor tests** fail in isolation too: 10 failed, 19 passed, 10 skipped in 5.4 s (action `fc6b34a4a67f`). Each failure prints `[filesystem-floor] refused uuid:44477634-83fe-4d08-bf42-1b037c8942ea: below_floor free 6075478016 < floor 6871947674 + charge 0 + demand 2147483648`: the filesystem under dl380g10's test scratch had 6.08 GB free against a 6.87 GB floor (about 5 percent of a roughly 137 GB disk).
3. **Control:** the same file on gen11 `281006ef63`, which passed it at about 20:50Z, now fails the same 10 tests with 85 floor refusals; free space was 5,902,934,016 B, lower than ten minutes before (action `28ef1b7984ee`). `gate-evidence/gen11-floor-control.*`.

So the nine are the host's disk, not code. I did not find what fills the disk; my `df` probe never ran (it queued, then the output directory was deleted). I could not show this has the same cause as fleetgraph#21.

## Other gates
- Shape gate: action `6dfecfffd2de9a12b7a6cfabb48e2350ae9eb30c9e866f2c2dd4c6d2dfebdfbf`, 2 passed in 1002.61 s.
- Emitter gate in the dry run: 10 pbmcp readers inspected, 0 older than the cut-off, 0 hosts not inspectable.
