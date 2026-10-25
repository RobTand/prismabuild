# Changed test inventory

Compared with the original source at `322addff9d^`. There are 85 test functions before and after. One identical-input key self-comparison is deleted, and one fixture-boundary test is added with three cases. The remaining changed functions keep the same names.

| Test | Claim and current proof |
|---|---|
| `test_a_dead_relay_reaps_its_owned_action` | A broken relay fails closed without leaving its action behind (#514). |
| `test_a_deadline_requires_a_recorded_finite_action_end_time` | An old or malformed terminal relay record cannot bypass a deadline. |
| `test_a_profiler_that_exits_first_waits_for_the_action` | ``nsys --duration`` stops tracing and exits while the action runs on. |
| `test_a_registered_backend_wraps_the_child_and_its_output_is_ingested` | a registered backend wraps the child and its output is ingested |
| `test_a_timed_out_action_still_files_the_profile_it_reached` | The timed-out case is the one a profile is most wanted for. |
| `test_a_windowed_profiler_does_not_cap_the_action_lifetime` | A completed window leaves execution to its actual deadline policy (#514). |
| `test_an_unreaped_zombie_relay_fails_closed` | ``kill(pid, 0)`` is insufficient: zombies have already exited (#514). |
| `test_concurrent_actions_bind_independently` | Two simultaneous bindings take different rates without crossing. |
| `test_omitting_profile_leaves_the_key_untouched` | Identical input returns the same key. This was a self-comparison of two identical calls, so it needed no test. `test_profile_is_sealed_into_the_action_key` retains the meaningful profiled versus unprofiled distinction.  |
| `test_profile_fixtures_own_their_git_boundary` | An unrelated outer checkout cannot select this action's scratch parent. |
| `test_the_ending_a_worker_files_carries_the_profile` | Claim, run, finish -- and the reference is in the record on disk. |
| `test_the_output_lock_still_reaches_the_child_through_the_profiler` | The real backend and relay must inherit the output lock into the payload. |
| `test_the_sampling_backend_profiles_the_pipeline_member` | PB records the pipeline member with real py-spy and ingests it into CAS. |
| `test_hard_stop_during_supplement_preserves_primary` | hard stop during supplement preserves primary |
| `test_successful_profile_survives_unparseable_stdout_as_complete` | successful profile survives unparseable stdout as complete |
| `test_a_fifo_marker_neither_blocks_nor_certifies` | ``read_text()`` on a FIFO blocks; the bounded reader must not. |
| `test_a_killed_run_keeps_the_coverage_disclaimer` | The partial checkpoint shares ``ingest``, so it cannot claim coverage. |
| `test_a_swallowed_refusal_publishes_no_receipt` | The regression: before the fix this action published ``produced: true``. |
| `test_the_native_sampler_records_workload_frames` | A supported native route publishes a real workload sample and receipt. |
| `test_the_shim_does_not_block_on_a_fifo_marker` | A FIFO at the marker path must not stall the refusal. |
| `test_a_contained_deadline_preserves_the_partial_sample_profile` | a contained deadline preserves the partial sample profile |
| `test_a_flush_that_outlives_its_bound_ends_at_the_bound` | a flush that outlives its bound ends at the bound |
| `test_a_flush_that_outlives_its_bound_still_reaches_the_hard_stop` | a flush that outlives its bound still reaches the hard stop |
| `test_an_incomplete_scope_census_never_reaches_a_signal` | One observed match is not uniqueness when discovery reports errors. |
| `test_the_flush_opportunity_is_gated_and_fails_closed` | Unprofiled, expired and pidfd-less paths fall through to the hard stop. |
| `test_the_flush_opportunity_needs_no_request_read_after_launch` | the flush opportunity needs no request read after launch |
| `test_the_prelaunch_read_is_the_profile_authority` | One validated request read answers deadline and profile before launch. |
| `test_direct_execution_deadline_still_governs_after_a_profile_window` | The direct runner keeps its explicit deadline after nsys exits (#514). |
| `test_pool_term_during_window_settle_keeps_profile_and_reaps_action` | pool term during window settle keeps profile and reaps action |
| `test_profile_checkpoint_delay_uses_the_action_end_time_for_a_deadline` | Ingest latency cannot turn a late windowed action into a success (#514). |
| `test_unusable_early_report_does_not_leave_action_running` | unusable early report does not leave action running |

The JSON inventory records every before and after assertion. The following removals need explanation.

- The two native tests again run the real `sample` backend through `run_local_action`. They retain mode, backend, rate, schema and clean-route assertions where originally present. Both now require the named frame to occur in an actual recorded sample, and verify the CAS receipt.
- The two deadline tests compare `action_phase` with independently retained raw relay bytes read by the parser. They accept `launched` or `ended`; an ended action must have died from SIGTERM. This replaces a transient-state assertion that raced legitimate group cleanup.
- The output-lock child compares its pipe device and inode with the parent pipe. This proves the child inherited the correct object without scanning unrelated process descriptor tables.
- Fixed scratch-directory checks were obsolete after scratch became uniquely named. `test_profile_source_cleanliness_1459.py` proves cleanup and containment.
- The FIFO fixtures directly create the FIFO. Their behavior remains proven by no-blocking and negative-coverage assertions, so the FIFO self-check was removed.
- `scope.ambiguous` was maintained by removed fixture-side discovery. Dedicated exact-worker identity tests retain ambiguity rejection.
- Wall-clock loop assertions become inotify, socket, Event or pidfd watchdogs. They bound failure cleanup rather than assert performance. Fake-clock cases retain the exact wait budgets and termination order.
- The contained deadline fixture waits for sampler startup and payload readiness before delivering its injected deadline. The explicit startup assertion cannot independently fail after that barrier.
- The output-scope guard, unrelated to #2246, remains outside this PR. #2258 was merged separately.
