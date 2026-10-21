# Placement hermeticity and gate budget evidence for #1890

Pristine `origin/main` at `38eed1488e6e1a622da295dd69ae041f63da6c08` passes
all 349 placement tests with default scratch on both architectures. On x86 with
scratch under `/mnt/shared/pb-scratch`, it fails 28 tests and passes 321.
These are 28 of the 29 failures in TrustLadder's `run-cu2xh2uw` gate.
The extra gate failure is the ordinary blocked-offer subprocess's 3-second
outer timeout. It did not recur in the pristine-main reproduction. The
first branch shared-scratch run reproduced the same measurement defect as
`offer discovery took 2.17s` because its stopwatch included CLI startup.
That run passed the other 348 tests, including all 28 repaired cases.

## Cause and fixture boundaries

The 28 reproduced failures depend on scratch location. Their purported local
executables and input files are siblings of the checkout beneath `tmp_path`.
TrustLadder put `tmp_path` under `/mnt/shared/tl-scratch`, so
`submission.command_dependency_contract` correctly classifies those paths as
shared. Shared paths need no host pin or per-host local-dependency answers.
That explains the empty placement, successful class proof despite an absent
local answer, missing dependency keys, and missing refusals.

This is test drift, not a demonstrated product bug. No production code,
placement default, production timeout, or shard partition changes in this branch.

The file now declares its placement environment before each test. Its private
`SHARED_ROOT` models shared dependencies while sibling fixture files model local
inputs, regardless of the mount holding pytest scratch. Tests for actual shared
path spelling continue to exercise `pool.is_box_local_path` through the original
production rule. Existing retained-alias tests override the private shared root
with their own fixture subtree.

The environment reads and their owners are:

- Roster: the global submission fixture formerly used
  `tools/fleet/fleet_boxes.json` beside the imported client. The file-level
  fixture supplies a private three-host JSON roster. Class tests keep their
  own two-member, missing, stale, or invalid rosters.
- Offers and CAS: `tests/conftest.py::_explicit_submission_store` already
  redirects both to `tmp_path`. The new fixture explicitly follows it.
  No default workers are announced. Each test publishes its own capacity,
  tags, capability answers, and freshness evidence through `PoolQueue`.
- Hostname and transport: the fixture supplies `sparky` and `pool`.
  Class-specific tests replace the hostname explicitly.
- Interpreter and dependency answers: class fixtures declare the executable
  path being tested through `InterpreterAnswers` and their explicit per-member
  local-dependency maps. The symlink tests intentionally use the admitted
  interpreter or local system executable as a target, then supply private offer
  answers for that exact path. No live worker is queried.
- Blocked-offer subprocess: its `FleetStore` now receives the declared runtime,
  private roster, and private runtime-version path directly. Previously it
  captured the source runtime before assigning `pbrun.RUNTIME_ROOT`, and used
  the checkout roster. Its FIFO and offer-read timeout assertions remain.
  A pipe notification now
  starts the discovery stopwatch at the real `bounded_offer_snapshot` call.
  A separate 30-second startup guard covers imports and Git inspection; the
  discovery keeps its 3-second outer guard and 2-second elapsed assertion.
- No test in this file needs `@pytest.mark.live_store`. The existing call-time
  live-store guard remains active for every test.

The following node IDs all fail on pristine main with shared scratch because
of the scratch-dependent classification. They pass with default scratch on
x86 and ARM. Prefix every line with `tests/test_pbrun_placement.py::`.

```text
test_portable_snapshot_keeps_a_box_local_executable_host_pin
test_portable_snapshot_resolves_a_bare_executable_through_declared_path
test_portable_snapshot_pins_a_direct_flag_value_outside_the_snapshot
test_portable_snapshot_screens_caller_environment_paths
test_default_class_cli_cases[venv]
test_default_class_cli_cases[host-only]
test_default_class_cli_cases[celestia-gpu]
test_class_default_screens_all_declared_paths[argv]
test_class_default_screens_all_declared_paths[environment]
test_class_default_includes_direct_dependency_evidence[argv-True]
test_class_default_includes_direct_dependency_evidence[argv-False]
test_class_default_includes_direct_dependency_evidence[environment-True]
test_class_default_includes_direct_dependency_evidence[environment-False]
test_default_cli_requires_real_external_targets[missing-argv]
test_default_cli_requires_real_external_targets[missing-environment]
test_default_cli_requires_real_external_targets[dangling-argv]
test_default_cli_requires_real_external_targets[dangling-environment]
test_default_cli_requires_real_external_targets[file-argv]
test_default_cli_requires_real_external_targets[file-environment]
test_default_cli_requires_real_external_targets[directory-argv]
test_default_cli_requires_real_external_targets[directory-environment]
test_default_cli_captured_execution_cannot_rebind_a_bare_executable
test_symlink_parent_traversal_refuses_actual_missing_input[argv]
test_symlink_parent_traversal_refuses_actual_missing_input[environment]
test_symlink_parent_traversal_guards_the_requested_input[argv]
test_symlink_parent_traversal_guards_the_requested_input[environment]
test_raw_file_suffix_is_invalid_at_capture[argv-/]
test_raw_file_suffix_is_invalid_at_capture[environment-/.]
```

The extra gate-only node is
`test_blocked_worker_offer_read_does_not_publish_a_runnable_submission[False]`.
Its traceback reports `subprocess.TimeoutExpired`, then the test's own
`pbrun wedged while reading worker offers` failure. There is no evidence here
that its production offer-read timeout failed; the outer timeout includes
interpreter startup and submission preparation.

## Remaining-budget failures in the three gate reports

The source reports were copied read-only from dl380g10:
`/mnt/shared/trustladder-gates/prismabuild/run-<run>/portable-x86.json`.
The lists below select only output lines containing both
`exceeded the per-test bound` and `The bound was tightened`.
Ordinary 180-second per-test failures are excluded.

`pytest_test_bound.remaining_bound` uses
`max(1, min(test_bound, shard_budget - elapsed - 30))`.
The alarm applies separately to setup, call, and teardown. Both exhausted
shards sealed `PRISMABUILD_TEST_TIMEOUT_S=180` and
`PRISMABUILD_SHARD_BUDGET_S=2400`; their failed terminal records confirm
`status=timeout` on dl380g10 at about 2408.5 seconds, including containment.

### run-cu2xh2uw, shard 5

Action `c00787b860bce377c1963e29e24c4b020dcf1eb4fe886331ebde2b5a702df10a`. Predicted 368.7 seconds; 34 distinct budget-tightened nodes.

```text
tests/test_install_ram_mount_unit.py::test_private_install_refuses_existing_unit_without_migration[different] | 23.6467s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_existing_unit_without_migration[symlink] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_existing_unit_without_migration[directory] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_existing_unit_without_migration[fifo] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_existing_unit_without_migration[mode] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_existing_unit_without_migration[hardlink] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_existing_unit_without_migration[owner] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_dropins_vendor_masks_and_generated_conflicts[etc/systemd/system/ram-prewarm.mount.d] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_dropins_vendor_masks_and_generated_conflicts[etc/systemd/system/ram-.mount.d] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_dropins_vendor_masks_and_generated_conflicts[etc/systemd/system/mount.d] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_dropins_vendor_masks_and_generated_conflicts[run/systemd/system/ram-prewarm.mount] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_dropins_vendor_masks_and_generated_conflicts[usr/local/lib/systemd/system/ram-prewarm.mount] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_dropins_vendor_masks_and_generated_conflicts[usr/lib/systemd/system/ram-prewarm.mount] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_dropins_vendor_masks_and_generated_conflicts[lib/systemd/system/ram-prewarm.mount] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_dropins_vendor_masks_and_generated_conflicts[run/systemd/generator/ram-prewarm.mount] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_dropins_vendor_masks_and_generated_conflicts[run/systemd/generator.early/ram-prewarm.mount] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_dropins_vendor_masks_and_generated_conflicts[run/systemd/generator.late/ram-prewarm.mount] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_unreadable_conflict_observations[etc/fstab] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_unreadable_conflict_observations[run/systemd/system/mount.d] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_unreadable_conflict_observations[run/systemd/generator/ram-prewarm.mount] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_fstab_mountpoint[/ram/prewarm] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_fstab_mountpoint[\\057ram\\057prewarm] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_fstab_mountpoint[/ram/prewarm/] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_fstab_mountpoint[/ram//prewarm] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_fstab_mountpoint[//ram/prewarm] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_fstab_mountpoint[/ram/./prewarm] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_fstab_mountpoint[/ram/other/../prewarm] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_writable_ancestors[] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_writable_ancestors[etc] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_writable_ancestors[etc/systemd] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_writable_ancestors[etc/systemd/system] | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_requires_existing_system_directory | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_does_not_change_simulated_current_mount_contents | 1s during setup
tests/test_install_ram_mount_unit.py::test_private_install_refuses_foreign_directory_owner | 1s during setup
```

### run-ynxhxna3, shard 5

Action `50b0b96afd306c7df260eb3d2962317ad7d8b436d1202cd95ea7be049a71299c`. Predicted 368.7 seconds; 6 distinct budget-tightened nodes.

```text
tests/test_produced_spool.py::test_local_group_exports_as_real_pb_action_then_stages_and_leases | 1s during call
tests/test_produced_spool.py::test_export_admission_respects_live_producer_aggregate_memory[96-True] | 1s during call
tests/test_ram_shared_source.py::test_delayed_fragment_adoption_never_replaces | 1s during call
tests/test_ram_shared_source.py::test_inflight_copy_defers_without_replacing | 1s during call
tests/test_slurm_outcome_records.py::test_a_job_that_published_a_receipt_is_filed_under_done | 1s during call
tests/test_slurm_outcome_records.py::test_a_job_that_finishes_before_scancel_lands_is_still_a_withdrawal | 1s during call
```

### run-nv9vgxqq

No explicit budget-tightened failure appears in the report. Eight shards have no action key because checkout identity inspection failed with a 30-second `git diff-index` timeout before submission. The placement shard is among those eight, so that run supplies no placement verdict. Its ordinary 180-second alarms do not establish remaining-budget exhaustion.

No exact budget-tightened node ID occurs in more than one of these three
reports. The recurring failure is exhaustion of shard 5, containing 57 files,
in `run-cu2xh2uw` and `run-ynxhxna3`; the affected test changes with run progress.
The counts above are observed alarms, not complete test outcomes: those two
shards end without verified completion records.

## What the partition evidence establishes

The history-based predictions leave substantial forecast headroom. In both
exhausted runs, shard 5 predicts 368.7 seconds, only 15.4% of its 2400-second
budget. Shards 1 through 19 each predict about 368.7 seconds. The isolated
checkout-recovery shard predicts 1438.4 seconds. In `run-nv9vgxqq`, the packed
predictions are about 368.5 seconds.

Actual time is less balanced. In the two exhausted runs, the other packed
shards with pytest summaries take about 247 to 1048 seconds, while shard 5
runs out its budget. In `run-nv9vgxqq`, an observed packed shard reaches
1874.06 seconds against a 368.5-second prediction. The source packer isolates
files predicted at or above half the deadline, then assigns the remaining
files longest-first to the least-loaded bucket. It does not forecast these
shards close to the budget; the retained history underpredicts their observed
work under this lane's conditions. These reports alone do not isolate disk
latency from contention, collection, fixture, or subprocess costs.
Partitioning is unchanged. Raising budgets or retrying the individual tests
would not explain the inaccurate predictions.

## PB validation commands and action records

Each run below uses the published client, default scratch unless stated,
2 CPUs and 4 GiB per shard, one native thread, and zero GPUs. The three-file
runs decompose into independent placement, duplication, and doc-reference
shards, with up to 6 CPUs and 12 GiB aggregate demand per run.

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/fleet/pbtest.py \
	--checkout CHECKOUT --python PYTHON LANE_OPTIONS \
	--threads-per-shard 1 --cpus-per-shard 2 --mem-gb 4 \
	--json /tmp/pb1890-RUN.json \
	tests/test_pbrun_placement.py tests/test_duplication_baseline.py \
	tests/test_design_doc_line_references.py
```

`main-*` uses `/tmp/pb1890-main`, a detached pristine `origin/main` checkout.
All other run labels use `/home/rob/.t3/worktrees/prismabuild/t3-9f98a30d`.
x86 uses `/home/rob/venvs/pb-cpu/bin/python3.14 --tag x86`.
ARM uses `/home/rob/venvs/pb-cpu/bin/python3.12` without a tag.
Shared-scratch runs add `--tmpdir /mnt/shared/pb-scratch` and select only
`tests/test_pbrun_placement.py`.
Runs named `final-*`, `verified-*`, and `accepted-*` use `--pytest-args '["--tb=line"]'` to distinguish them
from the initial validation snapshot. The final committed-source results
are the `accepted-*` runs. The initial branch runs precede the
last subprocess runtime-version and explicit transport fixture edits.
The `final-*` runs precede the pipe-based discovery stopwatch correction.
The `verified-*` runs caught a fixture wiring error: the child read its pipe
descriptor after `sys.argv` had been replaced for the CLI. The committed
source captures the descriptor first. Those intermediate failures are not
pristine-main baseline failures; the `accepted-*` runs validate the correction.

### main-x86

- `tests/test_pbrun_placement.py`: 349 passed in 48.75s; action `8cf50587e7853668b2eff727ab892a2674806afa0c2d34ff379cc8e52b743483`.
- `tests/test_duplication_baseline.py`: 8 passed in 27.49s; action `f0bebe24d2f1308e10b43cd5bb1b9ed6c2bb42fef7cb552e021bcf57ed23d872`.
- `tests/test_design_doc_line_references.py`: 1 passed in 0.22s; action `73fce1888fea060822ad6d7b806570262625aa580a6b3c0cd231b66ec2f4dd21`.

### main-arm

- `tests/test_pbrun_placement.py`: 349 passed in 22.81s; action `c0374fcea136111b9bfcc1be129bc6e6240a285202cce877ba9d41cce647180e`.
- `tests/test_duplication_baseline.py`: 8 passed in 9.23s; action `8827d566eb342dd80975dcc7c849b35965fcfbf61407b930c554729ed48cb053`.
- `tests/test_design_doc_line_references.py`: 1 passed in 0.08s; action `0816596fcdebd162ab4ec7b8e4ac2a5571e2bf970298f8df4f995c3f43ee7a8d`.

### main-shared

- `tests/test_pbrun_placement.py`: 28 failed, 321 passed in 57.52s; action `b92ed88e7c9895096db6c99ef9c686375eeb2b7b2eb86337764578ad2f5e64fb`.

### branch-x86

- `tests/test_pbrun_placement.py`: 349 passed in 66.94s (0:01:06); action `27dda37517fc268391aed8b19eeabfb07ca31f30d78776131f6d5dcd421c1bcd`.
- `tests/test_duplication_baseline.py`: 8 passed in 27.93s; action `3176d169948b5c2d9f6453fab542b4d44e646d56ea4d4a237e359309f35492dd`.
- `tests/test_design_doc_line_references.py`: 1 passed in 1.88s; action `910144244241ef810a66bf58523e2c65c37a9f3b202cfd0a0fa7aaaf460467fe`.

### branch-arm

- `tests/test_pbrun_placement.py`: 349 passed in 33.88s; action `6c625ec352c771fb0b5f0ff5b132c8fba5ed169e5ad1a97b216243530b998a7d`.
- `tests/test_duplication_baseline.py`: 8 passed in 9.21s; action `edd12cb37c5799757bddcc0e5d0ca4b9c196f0b729d9935774142a911a85091d`.
- `tests/test_design_doc_line_references.py`: 1 passed in 0.08s; action `873770e726236ce97c4a701a3a1de6aa3bf00005d27f658e20244be2d76beea0`.

### branch-shared

- `tests/test_pbrun_placement.py`: 1 failed, 348 passed in 64.00s (0:01:04); action `5f1b967eb16edae02ebfe15104e05d41a5633739c6644c37f3791acdcec911a3`.

### final-x86

- `tests/test_pbrun_placement.py`: 349 passed in 52.20s; action `4ded3d04bbe8de02c37b12a64d0e844d59f59e3fdb1dd01bb211005092ae5946`.
- `tests/test_duplication_baseline.py`: 8 passed in 25.73s; action `ebd91abc38534f42cfd7a3f70c18a4ab3af079a65475c2047ce954001a346809`.
- `tests/test_design_doc_line_references.py`: 1 passed in 0.75s; action `2661d9d52a44f60b3045b33e5e1d582491beffce8b1a571ca9b4d09dc3b7b94f`.

### final-arm

- `tests/test_pbrun_placement.py`: 349 passed in 21.04s; action `3ae2a0d6fdcca39601ee3a5dde5d5663d5a43fe69e83bd2da15cff3f793dc519`.
- `tests/test_duplication_baseline.py`: 8 passed in 9.28s; action `182536b00d1dbdf944d71031156551c9783d487f0ddd640fa0bd40f485127fb5`.
- `tests/test_design_doc_line_references.py`: 1 passed in 0.08s; action `c71de7534a3c983f691be006f321ac860ab6b5c2fbbb7106c7a8a766fb8bc3f6`.

### verified-x86

- `tests/test_pbrun_placement.py`: 2 failed, 347 passed in 53.02s; action `177db993e478f016108e78cb992a949e0f08e489c58186fb0eba1fef8f574d0c`.
- `tests/test_duplication_baseline.py`: 8 passed in 24.46s; action `977bc67ee2663e4362be5d73f26136609031ede20087631aae38314964bb82b1`.
- `tests/test_design_doc_line_references.py`: 1 passed in 0.34s; action `520b24234429f7da8d864d3dedb86d2790bbf9bf9ed4400dfe1b7082fe31cdfe`.

### verified-arm

- `tests/test_pbrun_placement.py`: 2 failed, 347 passed in 21.54s; action `cc591a6d8b0704961d7765a1fcf9da44cd1702f35d98ae5c25bd5ba3937974a0`.
- `tests/test_duplication_baseline.py`: 8 passed in 9.32s; action `b9fd2c086bb8d6925451d447682ba5e9008f71150faedc6b948f9c0a3dd5311e`.
- `tests/test_design_doc_line_references.py`: 1 passed in 0.09s; action `89fed418dc434eeb05e30a715eecc93a149bf9072f3c8b53500dd16217a75199`.

### verified-shared

- `tests/test_pbrun_placement.py`: 2 failed, 347 passed in 47.01s; action `d179bf9a86b2ab1c2f6b51acb9ae14ec859b212d701bc1ba0765959cfe4d91f1`.

### accepted-x86

- `tests/test_pbrun_placement.py`: 349 passed in 66.82s (0:01:06); action `05ac0bcaab34cc8c38a867183cf2bf47fb2c1b69f49ef1ddffaee2f66bd7a9cf`.
- `tests/test_duplication_baseline.py`: 8 passed in 29.24s; action `fd3ac2d0caee659776fe25e3b0a80ddd022413a267f6f6ce4a81f7a5220aae1b`.
- `tests/test_design_doc_line_references.py`: 1 passed in 0.28s; action `372ed2e93e6202fa6e063078bcbd61a1a73a37d6c7c9d150ad02e34bb0cc3403`.

### accepted-arm

- `tests/test_pbrun_placement.py`: 349 passed in 25.00s; action `94a55512fb907feaf710f7464708631a3dd59ebf5b692bba534f647e2035aa91`.
- `tests/test_duplication_baseline.py`: 8 passed in 9.75s; action `1b6ec20c0c83480cd36479036fe248eb405a5bc820e3d4dc5b6f3060321d9055`.
- `tests/test_design_doc_line_references.py`: 1 passed in 0.08s; action `5a831114632ac47b18871f80bcaa33b3a31d5e732c22ae8f9eeb1be7cfff656f`.

### accepted-shared

- `tests/test_pbrun_placement.py`: 349 passed in 55.08s; action `5f60f422083eb97ac538956eb2541abbc2235fa2bebf72e6ef17368286543822`.

## Receipt and source verification

All 34 validation action records above were inspected in the live PB queue.
Each successful action has a `done` terminal and a canonical CAS receipt.
`PrismaBuildCAS.lookup` verified each receipt's action binding, manifest,
attestation, digest, immutable payload mode, byte count, and payload digest.
The receipt-backed payload contains the reported pytest summary. Failed test
actions have failed terminals and no success receipt.

The `accepted-*` placement records bind source commit
`f8bc4e4247251eec877192d608cf003deda382db`. Their runtime records contain the
same placement-file SHA-256 as the committed source:
`3faad5bab739a64aed6ec72db15f1675ed59c22988082b87fd6378b14fee0fdc`.
The report-only follow-up commit adds these observations.

The final source passed 349 placement tests on dl380g10 with default scratch,
349 on sparky with default scratch, and 349 on dl380g10 with shared scratch.
All 9 supporting checks passed on both architectures. There were no skips.
The detached pristine-main worktree was removed after its runs completed.
Full-suite execution, runtime publication, and fleet deployment were not verified.
