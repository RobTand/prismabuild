# Hosted Actions merge gate

Ordinary PRs run `pr-gate` on GitHub-hosted `ubuntu-24.04`, selecting tests by
evidence. Mergify batch drafts run the full eligible population when the head
branch starts `mergify/merge-queue/`, the PR author is login `mergify[bot]` with
id 37929162, and the head repository is this repository. A same-repository PR
with the `gate-canary` label also runs the full gate, without duplicating the
single-job PR selection run. The label takes effect on the next push or reopen:
label changes don't trigger the workflow, because Mergify's own `queued` and
`dequeued` labels would otherwise rerun `pr-gate` on queued pull requests. No CI
job uses the fleet.
Mergify requires `pr-gate` to queue and the final job `gate` to merge, enforced
by the batched queue configuration merged in #2453. Full queue coverage
depends on `gate` succeeding on authenticated batch SHAs, not on a check of
an individual source PR.

The full plan records a shadow selection while coverage stays full. The full
matrix defaults to sixteen shards, alternating `ubuntu-24.04` and `ubuntu-24.04-arm`.
Workflow reruns swap the architectures by shard index, so qualification exercises
each file on both architectures.
`GATE_RUNNERS` is the workflow's single list of test destinations. The small
label-resolution job bootstraps on hosted x64; test jobs use its outputs,
and `matrix --runners` receives that same list. Changing test destinations
requires one edit to `GATE_RUNNERS`.
`GATE_SHARDS` is the single workflow setting for the full shard count. The
lead sizes Mergify parallel checks so shard count times concurrent batches
stays near the account's 20 hosted Linux jobs.
Each shard uses four pytest-xdist workers within inherited CPU affinity.
Actions are pinned by commit SHA. Setup uses Python 3.14 and pip caching with
pytest 9.1.1, pytest-xdist 3.8.0, PyYAML 6.0.3 and setuptools 80.9.0.
Hosted setup installs bubblewrap and creates the real local scratch directory
declared in the fleet roster. It exports that directory as TMPDIR. Proof capture
runs without setup-python's ambient LD_* loader overrides; the interpreter must
also pass its version check without those overrides. Native threads are bounded to
one and each test has the integration tier's 300-second failure bound. A test over 60 seconds is reported as a cost issue for #2328, not a speed
verdict or an exclusion. Permissions are read only; checkout credentials are not persisted and no secrets are configured.

## Plans and partitioning

The selector includes FAST, changed tests, direct production importers, literal
path references and the test-helper import closure. It resolves named directory
bindings, __file__ parents, test-relative paths, Path constructors, joinpath and
os.path joins. Computed fixture names conservatively match their containing
directory; an unresolved base may retain a literal suffix. Changing conftest,
pyproject, verify, lane manifests or the gate's code/config selects the full
population. Documentation alone does not.

The shared partitioner is also used by the nightly full-suite workflow. It sorts
by descending duration, then path, assigning each file to the least-loaded shard
with the lowest index breaking ties. Unknown files cost the nearest-rank p90 of
known costs; empty or all-zero inventories use one second. Unknown file costs
remain identifiable. Every file is retained exactly once, and the interface
retains empty shards when there are fewer files than shards.

```sh
python tools/ci/gate_select.py --base "$BASE_SHA" --head "$HEAD_SHA" --mode full \
  --output plan.json --files-output files.txt
python tools/ci/gate_partition.py matrix --files-from files.txt \
  --durations tools/ci/gate_durations.json --shards 16 \
  --runners ubuntu-24.04,ubuntu-24.04-arm --output matrix.json
python tools/ci/gate_partition.py shard --files-from files.txt \
  --durations tools/ci/gate_durations.json --shards 16 --index 0 \
  --output-files assigned.txt
python tools/ci/gate_run.py --plan plan.json --files-from assigned.txt --workers 4 \
  --result result.json --junitxml junit.xml
```

Each shard recomputes its assignment from the same population, duration table
and shard count. No test-file list passes through matrix entries. Coverage is
never truncated to meet a time budget. Two ten-shard trials took 6m20s and 6m21s from workflow start to the final
gate. The twelve-shard hosted-seeded run took 300 seconds. The first three
sixteen-shard runs took 267, 357 and 328 seconds, p50 328 and nearest-rank p90
357 seconds. These comparable runs retained the same 1,243-file population.
The qualification table below records the final population and setup. Sixteen
shards reserve eight jobs per architecture and leave the account some capacity. Timeouts include setup margin and never truncate coverage.

## Per-test cost limits

`tools/ci/gate_budget.json` sets a 20-second default. Its bootstrap slow list
uses hosted canary timings, not the PrismaBuild-worker inventory. Bootstrap runs
41240788649, 41242040262, 41242876374, 41243455746, 41244414908, 41245236880,
41245684824, 41246084806, 41246932853, 41247411735, 41247912349, 41248484796 measured
13 tests above 20 seconds. Every exception is `ceil(2 * largest_hosted_time)`,
ranging from 44 to 116 seconds. The policy records the maximum, run ID,
artifact name and verified download digest for each entry. These measurements
include an earlier 49.002-second duplication audit and 37.809-second history
audit, larger than the latest 21.952 and 34.428-second observations.

Issue #2448, under #2328, owns reducing these costs and shrinking the slow list.
The per-file scheduling table still uses the PB inventory's summed medians;
it estimates shard balance, rather than setting per-test cost limits.

The gate runner checks xunit1 JUnit times, including setup and teardown, after
pytest completes. Enforcement uses the runner's named
`BUDGET_ENFORCEMENT_TOLERANCE = 1.5`: a cost failure requires a time strictly
greater than the policy limit times 1.5. A test above its policy limit but at
or below that threshold passes with a warning in the result JSON, job summary
and JUnit suite output. For the default 20-second policy, times above 20 and
through 30 seconds warn; times above 30 seconds fail with the precise node ID.
Hosted-runner timing noise can push borderline tests just over their limits,
as the timing guard's 20.527-second run showed. The shrink-only policy cannot
add an exception for that borderline test, so tolerance belongs in enforcement.
The JSON limits and their ratchet remain unchanged; warnings keep creeping
costs visible for #2448.
The policy may only shrink versus `plan.base_sha`, the event's
`pull_request.base.sha`: neither the default nor an existing exception may
increase, and no new exception may be added. Removing an exception is allowed.
A target commit predating the policy permits bootstrap; an unavailable target
object fails with no verdict. Missing or malformed timing evidence also yields
no verdict, unless pytest already reported a test failure, which stays final.
Budget failures are final exit-1 verdicts even if pytest itself exited zero.
The 300-second test timeout remains a hang bound, separate from cost limits.
Budget enforcement runs only in this CI runner.

## Verdicts and retries

Exit 0 means pass, 1 means a test or cost-policy failure, and 75 means no verdict. Logs distinguish
`GATE_PASS`, `GATE_TEST_FAILURE` and `NO_VERDICT`. Setup/planning exceptions are
infrastructure failures. A shard's first attempt uploads its result and xunit1
JUnit with file attributes. The retry planner chooses only missing or invalid
verdicts, including a lost runner or failed setup. A test failure is never retried.
A separate job retries each chosen shard once, so retry can survive loss of the
first runner. A JUnit failure or error is final even if pytest then exits 2
or 3, or its timing evidence is incomplete. The final `gate` job fails on
explicit failure evidence from either attempt, even if the other passed, or
on a missing verdict after retry.

Artifacts are separated by workflow attempt and shard/attempt index to avoid
stale evidence on reruns. Plans include base/head SHAs, population version,
quarantine and duration provenance. Job summaries report selection/assignment
and verdicts. JUnit and final effective results remain available on failures.

## Shadow selection

Each full run archives the shadow selection and effective final shard records.
Compare selection against full-run failures with:

```sh
python tools/ci/gate_shadow.py --plan plan.json --results effective/*/result.json \
  --junitxml effective/*/junit.xml --output shadow-accuracy.json
```

The audit returns 0 for no missed observed failure, 1 for a missed failure, or
75 for incomplete evidence. It requires complete, unique assignments, actual
JUnit file coverage and matching population/base/head identities. Fully skipped
or empty files can make evidence inconclusive. Shadow accuracy does not reduce
the full gate population, and one successful audit proves only that run.

## Population and quarantine

The gate excludes root, GPU and dedicated two-CPU lanes declared by
`tools/verify.py` and the proof/receipt manifests. It excludes whole files in
`gate_quarantine.json` and deselects its node-prefix entries. Every quarantine
entry needs an open owning issue, a specific reason and evidence. Readiness
tracker #2428 is not an owner.

`environment_lanes` records capability requirements separately from quarantine.
It supports complete files and precise node prefixes, retaining portable tests
in mixed files. The hosted trial identified published-runtime dependencies
under #1182 and broker/worker fixture requirements under #2442. These controls
stay in their capability lane; they are not classified as flaky. A three-file
PrismaBuild comparison passed all 109 tests with verified CAS receipts, while
the hosted trial failed representative cleanup/scratch controls. The gate does
not install a fleet supervisor or broker, fabricate a shared runtime, or relax
production containment to make those controls pass.

Both `test_record_compatibility.py` and
`test_current_receipt_compat_composition.py` are included. Hosted canary
41247411735 passed all 24 compatibility cases and eight selected-interpreter
controls on both architectures. Initial trials refused UID-map setup; disabling
Ubuntu's AppArmor userns restriction allowed namespace entry. The compatibility
sandbox intentionally hides `/opt`, so setup copies Python's installed prefix
under root-owned `/usr/local/pb-gate-python`, preserving interpreter/library
bytes, registers its shared library with ldconfig, and relocates its sysconfig
installation metadata to the copied extension-module directory. The two
previously deferred customization controls also return to the gate. This is
hosted-VM setup, not a change to the test owners or their attestation checks.
The selection agent's admitted PB actions instead refused `/proc` mounting with
exit 78. That worker environment difference does not exclude passing hosted
tests. Run 41247912349 also passed five fresh 32-case repeats per architecture,
including both complete compatibility files and all eight interpreter controls.
Earlier namespace qualification work remains recorded in #2447.
The three flaky bundle-cache cases under #2435 exhaust their file's collection,
so the quarantine names that complete file.

Hosted privilege checks are measured, not inferred. Runs 41245684824,
41246084806, 41246932853 and 41247411735 record the AppArmor, interpreter,
loader and extension-module diagnoses. The final setup passed on x64 and ARM.
No fleet service or shared mount was changed. Temporary capability diagnostics
were removed after this classification.

The same canaries measured the complete 227-case root population with real
passwordless sudo. Earlier x64 runs passed 226 cases and ARM passed 224; run
41246932853 passed 224 on both. Both require the selected
`proof_container_runtime.run` wrapper for the installed-reader credential-drop
case, and ARM also failed two real-census assertions. Sudo itself works. The
authoritative root lane remains intact under its pinned proof-runtime wrapper
through PrismaBuild, owned by #1831, rather than pretending the bare-host run
provided that runtime attestation. Root privileges alone are not the remaining
requirement, and these root cases are not classified as flaky by this gate.

Remove an entry after root repair or five fresh passing repeats on each
architecture in the CI environment, as the owning issue requires. Quarantine
contains a failure; it does not repair it. Inventory durations are summed
per-test medians at 22e9d752. New files retain unknown inventory costs, and
partly quarantined files retain conservative whole-file estimates. Replay and
pool.py AST diagnostics are in `tools/ci/gate_replay.py`; function-level selection
remains follow-up work.

## Path-loading survey

```sh
python tools/ci/gate_paths.py --root . --output path-survey.json
```

Counts overlap and include temporary paths and generated fixture output; they
do not prove dependency completeness. Loader arguments contribute static path
evidence. with_suffix/glob operations retain conservative directory matches.
Paths supplied only by runtime arguments, environment or arbitrary helper return
values can remain unresolved without a static repository directory or suffix.
Queue coverage stays full while real-batch shadow evidence accumulates.


## Capability lane inventory

This table lists deferred files and the reason for each. Precise node-prefix
entries are in `tools/ci/gate_quarantine.json`; a mixed file keeps its portable
cases in the gate. No optional fleet CI job is installed: hosted jobs have no
shared fleet mount, and these existing PrismaBuild lanes provide admission,
the required inputs and verified CAS receipts.

| File | Lane | Reason | Owner | Execution |
| --- | --- | --- | --- | --- |
| `tests/qualification/test_cuda.py` | GPU | Requires a CUDA GPU. | verify.py | PrismaBuild GPU lane |
| `tests/test_a_batch_record_has_a_public_reader.py` | fleet_broker | Only 1 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_a_dead_producers_batches_and_paths_are_released.py` | fleet_broker | Only 3 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_a_dead_producers_held_mover_is_retired.py` | fleet_broker | Only 4 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_a_dead_producers_origin_files_survive_every_sweep.py` | fleet_broker | Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_a_dead_producers_spool_is_retired.py` | fleet_broker | Only 3 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_a_failed_producers_exports_do_not_block_its_successor.py` | fleet_broker | Only 3 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_a_hold_by_an_unknown_attempt_is_named.py` | fleet_broker | Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_a_kill_names_what_it_waited_on.py` | two CPU | Authoritative verify.py lane reserves two CPUs for controls that consume the assigned CPU pair. | verify.py | PrismaBuild integration two-CPU lane |
| `tests/test_a_nested_prefix_successors_adopted_file_survives_retirement.py` | fleet_broker | Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_a_read_back_owners_local_group_is_committed.py` | fleet_broker | Only 1 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_a_retry_reclaims_its_dead_attempts_prewrite.py` | fleet_broker | Only 4 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_a_successors_committed_identity_survives_a_link_back.py` | fleet_broker | Only 3 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_a_v2_read_plan_places_produced_batches.py` | fleet_broker | Only 3 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_an_identity_refusal_names_the_entry_and_both_identities.py` | fleet_broker | Only 1 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_an_origin_batch_survives_a_delegation_recall.py` | fleet_broker | Only 5 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_an_unpublished_declaration_is_released_once_nothing_can_publish_it.py` | fleet_broker | Only 1 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_claim_completion_histories.py` | fleet_runtime | Only 2 named controls are deferred; portable cases remain in the gate. Requires the real published runtime and wrapper under /mnt/shared/prismabuild-fleet/repo, absent on hosted workers. | #1182 | PrismaBuild lane with the recorded capability |
| `tests/test_claim_qualification_histories.py` | fleet_runtime | Only 3 named controls are deferred; portable cases remain in the gate. Requires the real published runtime and wrapper under /mnt/shared/prismabuild-fleet/repo, absent on hosted workers. | #1182 | PrismaBuild lane with the recorded capability |
| `tests/test_client_bootstrap_root.py` | root proof runtime | The declared root population runs together under its pinned proof-runtime wrapper; bare-host sudo is not the selected/attested installed-reader environment. | #1831 | PrismaBuild integration root lane |
| `tests/test_client_proof_current_trust.py` | root proof runtime | The declared root population runs together under its pinned proof-runtime wrapper; bare-host sudo is not the selected/attested installed-reader environment. | #1831 | PrismaBuild integration root lane |
| `tests/test_client_proof_metadata.py` | root proof runtime | The declared root population runs together under its pinned proof-runtime wrapper; bare-host sudo is not the selected/attested installed-reader environment. | #1831 | PrismaBuild integration root lane |
| `tests/test_client_proof_prior_trust.py` | root proof runtime | The declared root population runs together under its pinned proof-runtime wrapper; bare-host sudo is not the selected/attested installed-reader environment. | #1831 | PrismaBuild integration root lane |
| `tests/test_consumed_origin_retirement.py` | fleet_broker | Only 5 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_deferred_action_edges.py` | fleet_broker | Only 16 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_empty_retirement_intent_is_filed_before_unlink.py` | fleet_broker | Only 1 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_host_drain_evidence.py` | root proof runtime | The declared root population runs together under its pinned proof-runtime wrapper; bare-host sudo is not the selected/attested installed-reader environment. | #1831 | PrismaBuild integration root lane |
| `tests/test_installed_host_proof.py` | root proof runtime | The declared root population runs together under its pinned proof-runtime wrapper; bare-host sudo is not the selected/attested installed-reader environment. | #1831 | PrismaBuild integration root lane |
| `tests/test_installed_park_proof_loader.py` | root proof runtime | The declared root population runs together under its pinned proof-runtime wrapper; bare-host sudo is not the selected/attested installed-reader environment. | #1831 | PrismaBuild integration root lane |
| `tests/test_lifecycle_review_repairs.py` | fleet_broker | Only 3 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_local_scratch_core_recipes.py` | fleet_runtime | Only 5 named controls are deferred; portable cases remain in the gate. Requires the real published runtime and wrapper under /mnt/shared/prismabuild-fleet/repo, absent on hosted workers. | #1182 | PrismaBuild lane with the recorded capability |
| `tests/test_local_scratch_decorated_refusals.py` | fleet_runtime | Requires the real published runtime and wrapper under /mnt/shared/prismabuild-fleet/repo, absent on hosted workers. | #1182 | PrismaBuild lane with the recorded capability |
| `tests/test_local_scratch_measured_placement.py` | fleet_runtime | Requires the real published runtime and wrapper under /mnt/shared/prismabuild-fleet/repo, absent on hosted workers. | #1182 | PrismaBuild lane with the recorded capability |
| `tests/test_local_scratch_retained_review_boundaries.py` | fleet_runtime | Requires the real published runtime and wrapper under /mnt/shared/prismabuild-fleet/repo, absent on hosted workers. | #1182 | PrismaBuild lane with the recorded capability |
| `tests/test_missing_produced_input_terminal.py` | fleet_broker | Only 11 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_output_window_obligation.py` | fleet_broker | Only 2 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_park_epoch_injectivity.py` | root proof runtime | The declared root population runs together under its pinned proof-runtime wrapper; bare-host sudo is not the selected/attested installed-reader environment. | #1831 | PrismaBuild integration root lane |
| `tests/test_park_proof_real_writer.py` | root proof runtime | The declared root population runs together under its pinned proof-runtime wrapper; bare-host sudo is not the selected/attested installed-reader environment. | #1831 | PrismaBuild integration root lane |
| `tests/test_pbcanary_integration.py` | fleet_runtime | Only 1 named controls are deferred; portable cases remain in the gate. Requires the real published runtime and wrapper under /mnt/shared/prismabuild-fleet/repo, absent on hosted workers. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_pbcanary_legs12.py` | fleet_runtime | Only 1 named controls are deferred; portable cases remain in the gate. Requires the real published runtime and wrapper under /mnt/shared/prismabuild-fleet/repo, absent on hosted workers. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_pbrun_d38_gate.py` | fleet_broker | Only 1 named controls are deferred; portable cases remain in the gate. Deferred-release fixture needs the broker-backed producer cleanup to conclude; a hosted worker has no PB broker. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_pbrun_d38_setting_1649.py` | fleet_broker | Only 1 named controls are deferred; portable cases remain in the gate. Deferred-release fixture needs the broker-backed producer cleanup to conclude; a hosted worker has no PB broker. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_peer_placement_histories.py` | fleet_runtime | Only 7 named controls are deferred; portable cases remain in the gate. Requires the real published runtime and wrapper under /mnt/shared/prismabuild-fleet/repo, absent on hosted workers. | #1182 | PrismaBuild lane with the recorded capability |
| `tests/test_pool_cpu_tiers.py` | two CPU | Authoritative verify.py lane reserves two CPUs for controls that consume the assigned CPU pair. | verify.py | PrismaBuild integration two-CPU lane |
| `tests/test_prepaid_writer_integration.py` | fleet_broker | Only 1 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_produced_output_lifecycle.py` | fleet_broker | Only 1 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_produced_output_scope.py` | fleet_broker | Only 2 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_receipt_reader_root.py` | root proof runtime | The declared root population runs together under its pinned proof-runtime wrapper; bare-host sudo is not the selected/attested installed-reader environment. | #1831 | PrismaBuild integration root lane |
| `tests/test_record_compat_profile.py` | archival replay | The archival-replay capability requires retained runtime inputs absent from the gate image. Archival TMPDIR qualification is tracked by #1997. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller.py` | archival replay | The archival-replay capability requires retained runtime inputs absent from the gate image. Archival TMPDIR qualification is tracked by #1997. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_archival.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_broker.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_config.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_edges.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_floor.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_gang.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_inputs.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_markers.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_outcomes.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_preflight.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_prelaunch.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_prelaunch_release.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_prelaunch_turns.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_reclaim.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_rollout.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_slurm.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_state.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_controller_upgrade.py` | archival replay | Archival replay requires retained runtime inputs unavailable in the gate image and can refuse the admitted TMPDIR (#1997). Historical cost cluster #2253 is closed after splitting. Requalify each split before gate admission. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_core.py` | archival replay | The archival-replay capability requires retained runtime inputs absent from the gate image. Archival TMPDIR qualification is tracked by #1997. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_decomposition.py` | archival replay | The archival-replay capability requires retained runtime inputs absent from the gate image. Archival TMPDIR qualification is tracked by #1997. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_host.py` | archival replay | The archival-replay capability requires retained runtime inputs absent from the gate image. Archival TMPDIR qualification is tracked by #1997. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_outputs.py` | archival replay | The archival-replay capability requires retained runtime inputs absent from the gate image. Archival TMPDIR qualification is tracked by #1997. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_ownership.py` | archival replay | The archival-replay capability requires retained runtime inputs absent from the gate image. Archival TMPDIR qualification is tracked by #1997. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_queue.py` | archival replay | The archival-replay capability requires retained runtime inputs absent from the gate image. Archival TMPDIR qualification is tracked by #1997. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_compatibility_storage.py` | archival replay | The archival-replay capability requires retained runtime inputs absent from the gate image. Archival TMPDIR qualification is tracked by #1997. | #1997 | PrismaBuild archival replay with retained runtime inputs |
| `tests/test_record_store_liveness_controls.py` | fleet_broker | Only 5 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_required_park_identity.py` | root proof runtime | The declared root population runs together under its pinned proof-runtime wrapper; bare-host sudo is not the selected/attested installed-reader environment. | #1831 | PrismaBuild integration root lane |
| `tests/test_rollout_participant.py` | root proof runtime | The declared root population runs together under its pinned proof-runtime wrapper; bare-host sudo is not the selected/attested installed-reader environment. | #1831 | PrismaBuild integration root lane |
| `tests/test_scratch_lifetime_ownership.py` | fleet_runtime | Only 1 named controls are deferred; portable cases remain in the gate. Requires the real published runtime and wrapper under /mnt/shared/prismabuild-fleet/repo, absent on hosted workers. | #1182 | PrismaBuild lane with the recorded capability |
| `tests/test_spent_output_retry_disposition.py` | fleet_broker | Only 1 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_spool_poll_repin_evidence.py` | fleet_broker | Only 4 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_stage_stranded_funding_rolls_back_1555.py` | fleet_broker | Only 14 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_submission_ownership_c14.py` | fleet_broker | Only 1 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_submission_review_lifetimes.py` | fleet_broker | Only 2 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_superseded_origin_consumers.py` | fleet_broker | Only 1 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_the_origin_retirement_tick_reads_each_scope_once.py` | fleet_broker | Only 3 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_the_retirement_tick_reads_each_owner_once.py` | fleet_broker | Only 1 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_withdraw_end_to_end.py` | fleet_broker | Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |
| `tests/test_worker_disk_metadata_admission.py` | two CPU | Authoritative verify.py lane reserves two CPUs for controls that consume the assigned CPU pair. | verify.py | PrismaBuild integration two-CPU lane |
| `tests/test_write_only_produced_output.py` | fleet_broker | Only 2 named controls are deferred; portable cases remain in the gate. Broker scope fixture cannot conclude cleanup on a hosted worker without the PB broker; scope and fixture qualification is tracked separately, not as a flake. | #2442 | PrismaBuild lane with the recorded capability |

## Replay at the qualified population

The retained REST capture covers the latest 150 merged PRs as of 2026-10-31
19:28Z. Replaying it against the restored 1,245-file population selects p50
206 and p90 1,040 files; 52 PRs select more than 300 files and 36 more than
600. Twelve files lack inventory costs. Native cost estimates remain unknown
for selections containing them. Explicit p90 imputation at 7.11 seconds per
unknown file gives five-worker wall estimates of p50 181.990 seconds and p90
722.875 seconds, excluding setup and pickup. The full population has
3,832.80 known summed-median seconds; imputation gives 3,918.12 seconds and
783.625 seconds at five workers. These are PB inventory scheduling estimates,
not measurements of hosted CI wall time.

For the 26 captured PRs touching pool.py, base/head AST diffs found 1–49 changed
functions or methods per PR, p50 7 and p90 33, versus the original 714-file
module selection. This measures the potential of a follow-up; function-level
selection is not implemented. Source identities, per-PR counts, action keys
and verified receipt digests are in `tools/ci/gate_evidence.json`.

The path survey below counts occurrences and files. Categories overlap and
include generated and temporary paths. They measure idiom prevalence, not
selection accuracy.

| Idiom | Occurrences | Files |
| --- | ---: | ---: |
| `SourceFileLoader` | 10 | 7 |
| `glob` | 385 | 168 |
| `interpolated_string` | 4440 | 855 |
| `joinpath` | 26 | 13 |
| `open` | 334 | 161 |
| `os.path.abspath` | 5 | 4 |
| `os.path.dirname` | 6 | 4 |
| `os.path.join` | 9 | 5 |
| `os.path.realpath` | 11 | 6 |
| `path_constructor` | 3166 | 1097 |
| `posixpath.join` | 1 | 1 |
| `read_bytes` | 1940 | 474 |
| `read_text` | 1754 | 604 |
| `rglob` | 185 | 100 |
| `run_path` | 106 | 48 |
| `slash_join` | 20185 | 1307 |
| `spec_from_file_location` | 132 | 123 |
| `with_name` | 199 | 105 |
| `with_suffix` | 49 | 33 |

## Hosted qualification timings

The three final-population samples below each run all 1,245 eligible files.
They passed without infrastructure retries or test failures. No test above
60 seconds was observed in retained JUnit, including root/capability diagnostics. Full-run wall
measures workflow start to completion of the final `gate`; pickup measures
job creation to runner start. All times are seconds and GitHub timestamps
have one-second resolution.

| Run | Wall | Shard job range | Shard pickup range | Passed / skipped |
| --- | ---: | ---: | ---: | ---: |
| [41247912349, attempt 1](https://github.com/RobTand/prismabuild/actions/runs/41247912349/attempts/1) | 326 | 126–228 | 2–10 | 19,453 / 104 |
| [41248484796, attempt 1](https://github.com/RobTand/prismabuild/actions/runs/41248484796/attempts/1) | 305 | 121–219 | 2–12 | 19,453 / 104 |
| [41248484796, attempt 2](https://github.com/RobTand/prismabuild/actions/runs/41248484796/attempts/2) | 399 | 126–293 | 2–6 | 19,453 / 104 |

Nearest-rank p50 is 326 seconds and p90 is 399 seconds. The chosen
setting remains `GATE_SHARDS=16`, eight shards on each architecture. It uses
a large share of the account's hosted capacity as requested. The lead bounds
concurrent Mergify batches to leave total shard demand near 20 jobs.

A comparable earlier 1,243-file twelve-shard run, 41245236880, took 300 seconds,
with shard jobs 121–218 seconds and pickup 3–8 seconds. The first three
sixteen-shard trials at that same population took 267, 357 and 328 seconds.
These CI trials include setup and runner variation; they are not an isolated
shard-count performance experiment. Sixteen is the requested capacity default,
not a claim that these samples establish the optimum count. Setup variation
still affects the five-minute target; all qualified samples stay below the
ten-minute gate limit.

The first final-population sample included two temporary capability jobs.
Those jobs passed five fresh 32-case repeats per architecture and were then
removed. The second and third samples use the permanent workflow. Rerunning
swaps the architecture for each shard, so every file is exercised on both.

Each cell below is job duration / pickup. The first two samples assign even
indices to ARM and odd indices to x64. The third sample swaps that assignment.

| Shard | Run 41247912349 a1 | Run 41248484796 a1 | Run 41248484796 a2 |
| --- | ---: | ---: | ---: |
| 0 | 207 / 6 | 172 / 4 | 265 / 3 |
| 1 | 218 / 3 | 197 / 2 | 189 / 6 |
| 2 | 184 / 4 | 219 / 4 | 293 / 3 |
| 3 | 188 / 2 | 190 / 12 | 160 / 5 |
| 4 | 131 / 4 | 124 / 5 | 133 / 2 |
| 5 | 228 / 2 | 185 / 3 | 132 / 5 |
| 6 | 126 / 5 | 126 / 5 | 207 / 3 |
| 7 | 172 / 2 | 142 / 3 | 126 / 5 |
| 8 | 139 / 6 | 121 / 4 | 131 / 3 |
| 9 | 181 / 10 | 140 / 3 | 161 / 5 |
| 10 | 146 / 4 | 137 / 4 | 148 / 2 |
| 11 | 187 / 3 | 154 / 2 | 127 / 4 |
| 12 | 126 / 4 | 145 / 5 | 193 / 3 |
| 13 | 212 / 3 | 132 / 2 | 148 / 5 |
| 14 | 139 / 5 | 144 / 5 | 142 / 3 |
| 15 | 135 / 3 | 180 / 3 | 129 / 4 |

Attempt 1 of run 41248484796 lost six JUnit archives when its rerun began
before their download. All 16 archived job logs confirm its full pass/skip
totals, and GitHub retains complete per-job timing records. The other samples
have complete retained JUnit. No missing timing is treated as zero.

Control-job durations / pickup:

| Job | First | Second | Third |
| --- | ---: | ---: | ---: |
| Resolve test runner labels | 3 / 3 | 2 / 3 | 3 / 3 |
| Plan full gate | 48 / 2 | 30 / 3 | 40 / 10 |
| Plan infrastructure retry | 16 / 3 | 20 / 3 | 20 / 3 |
| gate | 16 / 2 | 17 / 2 | 18 / 3 |

JUnit ZIP digests, individual runner labels and all per-job measurements are
in `tools/ci/gate_evidence.json`. Shadow comparisons have complete assignments
and no missed observed failure; these gate implementation changes select the
full population even in shadow mode, so they do not qualify narrow selection.

Development verification used published PrismaBuild at priority -10. All changed
runner, budget, aggregator and selector test files passed five fresh repeats
per architecture, twice with xdist, 1,040 executions and 40 verified CAS
receipts. Five missing-basetemp setup failures were rerun successfully; the
final correction drops that flag and passes another 152 cases in five verified
actions. Failing actions have terminal records/logs and no success receipt.
No x86 admission check remains pending. Complete keys and receipt digests are
in `tools/ci/gate_evidence.json`.
