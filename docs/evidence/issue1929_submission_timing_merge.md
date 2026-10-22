# #1929 submission-timing merge qualification

## Scope

Resolve the remaining `pool.py` conflict when integrating main
`f8941f503f495b863267da0f870b7937e3226db5` into reviewed #1929 head
`82c5bdb42330dd0b72091f166ab97100a61ec26e`.

The immutable attempt retains both independent metadata groups:

- `retry_scope` and, for worker-reported outcomes, `worker_reported`, preserving
  interruption-only retry finality;
- main's optional `phases` snapshot, preserving submission-timing recovery.

The existing sealed interruption-policy regression now asserts that the READY
row, terminal row and immutable attempt retain the same timing snapshot, alongside
an immediately terminal first-attempt worker failure. Guest selection, borrowed-CPU
reclamation and cleanup serialization are unchanged by this repair. Incoming main
changes, including its compatibility goldens, were retained rather than regenerated.

## Executed checks

Tests ran through `run_tests` on its default admitted x86 / Python 3.14 arm.
An initial call specifying the arm name `x86` was refused by the tool before
execution (it exposes only the default arm); the commands below omit that argument.
Retained stdout for both operations was independently read back with
`read_test_artifact`.

### Timing, sealed retry policy and immutable attempts

```sh
python -m pytest tests/test_submission_phases.py tests/test_submission_phase_review.py tests/test_pbrun_retry_contract.py tests/test_pbrun_sealed_publication.py tests/test_pool_attempt_history.py -q --basetemp="$TMPDIR/pytest"
```

Result: **92 passed**, exit 0, 7 warnings (multi-threaded `fork` deprecation).
This includes timing recovery after supersession-archive removal, worker-failure
finality with timing, and lease-loss retry with resource-failure telemetry.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"c1911bcdb9379bbbfa1f7f0862e952f9df85137c20459958ecb88c1da8b89676","size":3119}},"operation":"d58e838421c3c258377010ab4c1d09353664fb175aed6dc34dbbaa68e923580f","receipt":"05d280d16c2140c447f7aa6dddf49473241b585ab1787fa38a268822106a4e4d"}]

### Guest scheduling, preemption, queue-bell integration and repository checks

```sh
python -m pytest tests/test_idle_gpu_cpu_guests.py tests/test_idle_gpu_proof_freshness.py tests/test_gpu_evicts_cpu_guests.py tests/test_borrowed_cpu_guest_eviction.py tests/test_local_scratch_measured_placement.py tests/test_preemption_handoff_admission.py tests/test_preemption_is_visible_to_readers.py tests/test_preemption_review_boundaries.py tests/test_pool_preempts_a_background_holder.py tests/test_deferred_action_edges.py tests/test_pbtest_seals_interpreter_1462.py tests/test_adaptive_cpu.py tests/test_adaptive_cpu_local_state.py tests/test_pool_cross_resource_placement.py tests/test_peer_placement_histories.py tests/test_queue_bell.py tests/test_pbrun_client_events.py tests/test_consumer_surface.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_pbrun_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

Result: **618 passed**, exit 0, 86 warnings (multi-threaded `fork` deprecation).
Includes fully borrowed and metadata-only guests, pending-cleanup serialization
across pressure-to-token handoffs, idle-evidence freshness, ordinary pinned
background preemption, retry lineage, inventory, CLI help, duplication and design
references.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"21f858972eb20a77c4e41089c0dfa20b7c057edd81cf7aa1e45ca71953f694f3","size":6628}},"operation":"2e8abbb5e890936d8029df4b06350c150c7a16eceae3ef6283fd23c888534d96","receipt":"f990bb3c772e60b9aee821402a054d004ac72826305b5323ca2732a25339c9e5"}]

## Limits and handoff

- This is source-level merge qualification: **710 passed** across the two runs.
  No full-suite, archival compatibility replay, ARM, deployment, live GPU canary,
  worker-convergence or physical crash-recovery claim is made.
- `git diff HEAD --check` passed and the final source diff was inspected. The
  `pool.py` delta from the reviewed head consists only of main's timing import,
  READY snapshot capture and immutable-attempt snapshot copy.
- The working-tree conflict markers are removed. `git add` could not mark the
  conflict resolved because creating the external worktree Git index lock was
  denied. No permissions or Git administration files were changed; recording the
  resolution and committing remain with TrustLadder.
