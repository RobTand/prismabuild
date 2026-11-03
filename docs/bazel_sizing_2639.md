# BuildBuddy test sizing, issue #2639

The sizing change gives measured CPU reservations to 73 remote test targets.
It does not fix timing-dependent tests. The 15 baseline assertion-failing
targets **passed when not starved** in calibration; their flake-fix issues
#2640 through #2650 remain open work.

Per-test budget overruns now produce warnings and never trigger a rerun
(#2676). The failure counts below retain the calibration's historical verdicts;
passing tests with those timings now remain passing. Hard Bazel `TIMEOUT`
results still fail, and the static budget-growth check still rejects increases.

## Calibration

Baseline [553ca9c3](https://app.buildbuddy.io/invocation/553ca9c3-e7ec-4764-910f-39e1d4d9b17f)
had 82 failed targets and eight timeouts in 955 seconds. Of the failures,
67 were budget-only and 15 had assertion failures. That is 23 distinct
assertion-failing or timed-out targets, despite the original README's count
of 24. Calibration selected all 90 failed or timed-out targets, plus tests
whose source launches children or imports process-backed broker fixtures.
Hosted-only targets were excluded.

The first [236-target probe](https://app.buildbuddy.io/invocation/3b33b38d-a0eb-46c9-b549-23aa4a3f5568)
requested four CPUs and 2 GB, but predictions or measurements replaced those
requests. It had 231 passing targets, five failures, and no timeouts in
338.705 seconds. It is diagnostic evidence, not the sizing calibration.

The corrected [236-target calibration](https://app.buildbuddy.io/invocation/c2df5862-efa0-464a-9226-9ed173029d0d)
used `EstimatedComputeUnits=1`, `EstimatedCPU=4`, `EstimatedMemory=2GB`,
`--jobs=16`, `--nocache_test_results`, `--remote_execution_priority=100`, and
`--run_under=//tools/bazel:calibrate_tests`. Receipts confirmed effective
4000m CPU and 2,147,483,648 bytes. It had 232 passing targets, four failures,
and no timeouts in 251.782 seconds. No failed target was rerun to obtain a
pass. The second calibration corrected an ineffective resource request.

Sixteen runner-lane targets share a runner cgroup and cannot provide isolated
per-action measurements. The other 220 targets are recorded in
[test_sizing_measurements.json](../tools/bazel/test_sizing_measurements.json).
The table reserves two CPUs for 65 targets, three for seven, and four for
one. The remaining 147 measured targets keep dynamic sizing. Memory is
0.5 GB except for allowance-suite isolation at 0.75 GB.

Process counts exclude the sampler and include launchers and pytest. They
are sampled lower bounds, not a count of simultaneously runnable processes.
CPU bursts are maximum cgroup CPU deltas over approximately 100 ms, whereas
`average_cpu` uses the receipt's `cpuNanos / execution wall time`. The
[recipe and derivation](bazel.md#measured-test-sizes) describe the rounding
and how to reproduce the measurements.

## Effective sizing receipts

Both receipts are available through `BuildBuddyService/GetExecution` with
`inlineExecuteResponse=true`. Inspect the auxiliary metadata record's
`schedulingMetadata`, rather than only the command's platform properties.

| Invocation and target | Requested CPU / memory | Effective CPU / memory | Cgroup |
| --- | --- | --- | --- |
| `3b33b38d`, `test_census_reused_once_per_pass_1571` | 4000m / 2,147,483,648 bytes | 1000m / 448,631,104 bytes, predicted | OCI, CPU weight 39, three leased CPUs |
| `c2df5862`, `test_a_produced_mover_seals_the_producers_checkout_snapshot` | 4000m / 2,147,483,648 bytes | 4000m / 2,147,483,648 bytes | OCI, CPU weight 157, six leased CPUs |

Both had the global 30-core quota `cpu.max=3000000 100000` and
`memory.max=max`. The estimate is a scheduling reservation and CPU weight,
not an estimate-sized CPU hard limit. The table uses nonzero compute units
to disable prediction and measured sizing, then explicit CPU and memory to
override the compute units' coupled defaults.

## The four corrected-calibration failures

| Target and node | Failed condition | Comparison with 553ca9c3 |
| --- | --- | --- |
| `test_a_measurement_admits_its_own_spool_exports::test_foreign_work_is_still_refused_and_the_refusal_names_the_holder` | Budget: 33.756 s exceeded the 20 s policy. Assertions passed. | Same node exceeded its budget at 37.020 s. |
| `test_a_stalled_mover_ends_no_progress::test_a_pool_held_in_the_pacers_release_band_is_credited` | `assert outcome["status"] == "executed"`; actual status was `failed`, with no termination reason and 238 replay rejections. | New assertion failure. This target previously failed two different nodes' budgets. |
| `test_core::test_action_key_binds_every_semantic_input[path1-codebook]` | `ActionContractError: action executable changed while it was read`, for the shared rules_python interpreter. | New failure; the target passed in the baseline. |
| `test_gang_bounded_loans_2343::test_unknown_presence_and_gaps_reset_without_a_bounded_candidate[partner_offer]` | Budget: 61.154 s exceeded the 20 s policy. Assertions passed. | New node overrun; the same target had three other budget overruns in the baseline. |

No retry, quarantine, skip, or larger timeout was added for these failures.
The calibration overlapped PrismaQuant's full run after 18:15Z, so its wall
times do not establish an uncontended performance comparison.

## Red and green

The RED commit adds checks for rule properties, measured table entries naming
existing tests, and unchanged properties, tags, and environment on unsized
targets. The [RED invocation](https://app.buildbuddy.io/invocation/5e93a1d2-1e60-4157-a81b-0d2b1fe67c5b)
failed because the sizing table was absent; both unsized checks passed.

The GREEN check also covers derivation from synthetic, fixed timestamps and
counters, with no real-time thresholds or sleeping in the tests.

The [GREEN invocation](https://app.buildbuddy.io/invocation/ad6765be-9e2d-4399-8bf2-d5d22d68317e)
passed all five selected targets: sizing contracts, workspace contracts,
BUILD generation, input handling, and population parity. It used
`--jobs=16` and `--remote_execution_priority=100`.

## Per-target reservations

Every row also sets `EstimatedComputeUnits=1`. CPU bursts are in cores.

| Target in `//tests` | Measured burst | Live processes | EstimatedCPU | EstimatedMemory |
| --- | ---: | ---: | ---: | --- |
| `test_a_blocked_copy_goes_first_on_its_tier` | 1.303 | 6 | 2 | 0.5GB |
| `test_a_hanging_test_fails_with_its_name_in_the_record` | 2.007 | 8 | 3 | 0.5GB |
| `test_a_produced_mover_seals_the_producers_checkout_snapshot` | 1.353 | 9 | 2 | 0.5GB |
| `test_a_ready_consumers_lead_is_read_first` | 1.297 | 6 | 2 | 0.5GB |
| `test_a_released_consumer_key_races_the_claim_in_two_windows` | 1.357 | 6 | 2 | 0.5GB |
| `test_a_replaced_progress_record_is_read_again` | 1.728 | 6 | 2 | 0.5GB |
| `test_a_republished_key_keeps_one_terminal` | 1.271 | 7 | 2 | 0.5GB |
| `test_a_shared_mount_row_without_a_data_manifest_is_warned_or_refused` | 1.359 | 7 | 2 | 0.5GB |
| `test_a_staged_range_holds_its_tokens_until_an_egress_deletes_it` | 1.373 | 6 | 2 | 0.5GB |
| `test_a_staged_wait_is_not_no_progress` | 1.739 | 6 | 2 | 0.5GB |
| `test_a_stalled_mover_ends_no_progress` | 2.667 | 11 | 3 | 0.5GB |
| `test_a_successors_committed_identity_survives_a_link_back` | 1.422 | 6 | 2 | 0.5GB |
| `test_a_v2_read_plan_places_produced_batches` | 1.442 | 6 | 2 | 0.5GB |
| `test_a_yielding_copy_neither_deadlocks_nor_stalls` | 1.633 | 6 | 2 | 0.5GB |
| `test_adaptive_cpu_local_state` | 1.298 | 7 | 2 | 0.5GB |
| `test_allowance_suite_filesystem_isolation` | 1.322 | 8 | 2 | 0.75GB |
| `test_an_owner_waiting_on_its_own_export_is_not_no_progress` | 1.418 | 7 | 2 | 0.5GB |
| `test_box_prune_race_1542` | 1.992 | 8 | 2 | 0.5GB |
| `test_claim_denial_batching` | 2.674 | 9 | 3 | 0.5GB |
| `test_claim_progresses_while_a_sibling_stalls` | 1.427 | 6 | 2 | 0.5GB |
| `test_cleanup_obligation_2270` | 1.517 | 7 | 2 | 0.5GB |
| `test_consumer_archived_endings` | 1.301 | 7 | 2 | 0.5GB |
| `test_consumer_surface` | 1.699 | 7 | 2 | 0.5GB |
| `test_contained_deadline_profile` | 1.719 | 9 | 2 | 0.5GB |
| `test_core` | 1.608 | 8 | 2 | 0.5GB |
| `test_cpu_control_plane_admission` | 1.632 | 7 | 2 | 0.5GB |
| `test_fixture_broker_process` | 1.592 | 7 | 2 | 0.5GB |
| `test_funded_window_declaration` | 1.563 | 7 | 2 | 0.5GB |
| `test_gang_bounded_loans_2343` | 1.833 | 7 | 2 | 0.5GB |
| `test_gang_plan_stat_uncertainty` | 1.918 | 7 | 2 | 0.5GB |
| `test_gang_plan_terminal_1543` | 1.538 | 7 | 2 | 0.5GB |
| `test_gang_residency_members` | 1.715 | 7 | 2 | 0.5GB |
| `test_gpu_sample_facts_profiles` | 1.482 | 7 | 2 | 0.5GB |
| `test_heartbeat_stamp_skew_2230` | 1.358 | 7 | 2 | 0.5GB |
| `test_measurement_census_reader_fence_1498` | 1.269 | 6 | 2 | 0.5GB |
| `test_measurement_drains_gpu_backfill` | 1.685 | 7 | 2 | 0.5GB |
| `test_measurement_reservation_census_1419` | 1.786 | 7 | 2 | 0.5GB |
| `test_missing_produced_input_terminal` | 1.280 | 6 | 2 | 0.5GB |
| `test_operator_host_hold` | 1.441 | 7 | 2 | 0.5GB |
| `test_ordinary_census_admission_2199` | 1.532 | 9 | 2 | 0.5GB |
| `test_paced_exports_share_a_tier` | 1.627 | 9 | 2 | 0.5GB |
| `test_pbcampaign_inflight` | 2.067 | 8 | 3 | 0.5GB |
| `test_pbtest` | 1.982 | 7 | 2 | 0.5GB |
| `test_pbtest_exit_completion_1530` | 1.998 | 8 | 2 | 0.5GB |
| `test_pbtest_identifies_its_checkout_once` | 2.846 | 8 | 3 | 0.5GB |
| `test_pbtest_reconciles_outcomes` | 2.006 | 8 | 3 | 0.5GB |
| `test_pbtest_seals_interpreter_1462` | 1.342 | 7 | 2 | 0.5GB |
| `test_pbwait` | 1.340 | 9 | 2 | 0.5GB |
| `test_placement_prefers_resident_copy` | 1.506 | 7 | 2 | 0.5GB |
| `test_pool` | 1.408 | 8 | 2 | 0.5GB |
| `test_pool_action_timeout` | 1.885 | 7 | 2 | 0.5GB |
| `test_pool_observes_the_daemon_spawned_child` | 1.555 | 5 | 2 | 0.5GB |
| `test_pool_receipt_reconciliation` | 1.644 | 7 | 2 | 0.5GB |
| `test_pool_reset_refusal` | 1.329 | 7 | 2 | 0.5GB |
| `test_pool_resource_scope` | 1.301 | 6 | 2 | 0.5GB |
| `test_pool_retry_survives_a_corrupt_claimed_record_1403` | 1.630 | 6 | 2 | 0.5GB |
| `test_pool_withdraw` | 1.531 | 9 | 2 | 0.5GB |
| `test_preemption_handoff_admission` | 1.292 | 8 | 2 | 0.5GB |
| `test_prepaid_output_funding` | 1.596 | 8 | 2 | 0.5GB |
| `test_produced_batches_keep_distinct_stage_paths` | 1.344 | 9 | 2 | 0.5GB |
| `test_progress_declared_allowances` | 1.389 | 7 | 2 | 0.5GB |
| `test_progress_reporting_boundaries` | 1.528 | 7 | 2 | 0.5GB |
| `test_progress_reporting_is_reachable_from_any_action` | 1.546 | 7 | 2 | 0.5GB |
| `test_proof_runtime_observation` | 1.466 | 6 | 2 | 0.5GB |
| `test_residency_is_derived_and_gates_admission` | 1.845 | 7 | 2 | 0.5GB |
| `test_retained_worker_proxy` | 1.599 | 7 | 2 | 0.5GB |
| `test_sample_profile` | 2.199 | 10 | 3 | 0.5GB |
| `test_scratch_declaration_record` | 1.469 | 7 | 2 | 0.5GB |
| `test_sealed_bundle_cache` | 3.015 | 11 | 4 | 0.5GB |
| `test_stale_mention_co_owner_cache` | 1.336 | 6 | 2 | 0.5GB |
| `test_the_claim_pass_lists_claimed_once` | 1.695 | 6 | 2 | 0.5GB |
| `test_the_nonfinal_window_fences_its_advance` | 1.292 | 6 | 2 | 0.5GB |
| `test_tier_funding` | 1.682 | 7 | 2 | 0.5GB |
