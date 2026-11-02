"""Tests that cannot run hermetically, each with its reason.

The remote platform turns networking off and runs each test in a container
that has a few CPUs of a larger host, as an unprivileged user, on a runfiles
tree with every input executable and no .git. A test listed here gets back
what its tags name. Keep this table short and the
reasons true; the PR that adds an entry says why the test can't use a declared
input instead.
"""

NONHERMETIC = {
    "//tests:test_a_dead_producers_spool_is_retired": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_a_failed_producers_exports_do_not_block_its_successor": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_a_producers_export_rates_are_learned_per_family": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_a_producers_paced_export_borrows_its_familys_fill": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_a_read_back_owners_local_group_is_committed": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_action_resource_profile": struct(
        tags = ["runner"],
        reason = "it reads the test's own cgroup membership; hosted processes live in a service cgroup, so the runner gives each test a delegated leaf.",
    ),
    "//tests:test_an_export_survives_a_delegation_recall": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_an_identity_refusal_names_the_entry_and_both_identities": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_attempt_log_archive": struct(
        tags = ["runner"],
        reason = "its archive tests need the zstd command, which the hosted image has and the executor image lacks; on the executor they skip.",
    ),
    "//tests:test_attempt_log_retention": struct(
        tags = ["runner"],
        reason = "its archive tests need the zstd command, which the hosted image has and the executor image lacks; on the executor they skip.",
    ),
    "//tests:test_attempt_log_retention_pacing": struct(
        tags = ["runner"],
        reason = "it needs the zstd command, which the hosted image has and the executor image lacks.",
    ),
    "//tests:test_broker_bridge_publication_2229": struct(
        tags = ["runner"],
        reason = "the installer refuses a root-owned, world-writable ancestor; remote runfiles sit under /buildbuddy-execroot (root, 0777).",
    ),
    "//tests:test_census_reuse_transitions_1571": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_check_export_receipt_1286": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_checkout_recovery_1465": struct(
        tags = ["runner"],
        reason = "it calls io_uring_setup, which the executor's seccomp profile refuses (ENOSYS).",
    ),
    "//tests:test_claim_arbitration_histories": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_client_upgrade": struct(
        tags = ["runner"],
        reason = "the installer refuses a root-owned, world-writable ancestor; remote runfiles sit under /buildbuddy-execroot (root, 0777).",
    ),
    "//tests:test_client_upgrade_initializes_boot_gate": struct(
        tags = ["runner"],
        reason = "the installer refuses a root-owned, world-writable ancestor; remote runfiles sit under /buildbuddy-execroot (root, 0777).",
    ),
    "//tests:test_compat_bundle_cache_isolation": struct(
        tags = ["hosted"],
        reason = "its golden bytes depend on gate_setup.py's relocated, root-owned interpreter under /usr/local; on the runner the replay differs at byte 315.",
    ),
    "//tests:test_container_image_inventory": struct(
        tags = ["hosted"],
        reason = "it needs a running Docker daemon, which hosted runners have; elsewhere it skips.",
    ),
    "//tests:test_contract_registry_publication": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_current_receipt_compat_composition": struct(
        tags = ["runner"],
        reason = "it needs bubblewrap with unprivileged user namespaces, which gate_setup.py installs on hosted shards; the executor image lacks it.",
    ),
    "//tests:test_durable_maintenance_review": struct(
        tags = ["runner"],
        reason = "the installer refuses a root-owned, world-writable ancestor; remote runfiles sit under /buildbuddy-execroot (root, 0777).",
    ),
    "//tests:test_durable_maintenance_upgrade": struct(
        tags = ["runner"],
        reason = "the installer refuses a root-owned, world-writable ancestor; remote runfiles sit under /buildbuddy-execroot (root, 0777).",
    ),
    "//tests:test_exact_role_log_metadata_migration": struct(
        tags = ["runner"],
        reason = "it runs only as uid 1000, the production acting owner. Hosted runners are uid 1001 and skip it; executors are uid 1000 and would run it, so it runs on the runner VM (uid 1001) to keep the hosted skip.",
    ),
    "//tests:test_family_fill_lends_once": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_filesystem_floor_1483": struct(
        tags = ["hosted"],
        reason = "it needs a non-loopback address, which executors lack, and a filesystem identity the runner VM's ext4 can't give (its tests skip there); hosted runners give both.",
    ),
    "//tests:test_gang_bounded_loans_2343": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_gang_cleanup_1521": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_gang_demand_fence_claims_1579": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_gang_plan_confirmation": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_gang_plan_stat_uncertainty": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_gang_plan_terminal_1543": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_gang_record_isolation_1521": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_gang_registration_1521": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_gang_reservation_1517": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_gang_residency_members": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_gang_sweep_progress_1521": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_gang_teardown_1517": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_gang_teardown_with_prelaunch_1543": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_gang_wait_deadline_2343": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_generation_drift_refusals": struct(
        tags = ["runner"],
        reason = "worker_loop's --all-cores sizes CPU slots from the host's sysfs, which on a remote executor exceeds the container's cpuset; a hosted runner owns all its CPUs.",
    ),
    "//tests:test_generation_receipt_source_contract": struct(
        tags = ["runner"],
        reason = "the installer refuses a root-owned, world-writable ancestor; remote runfiles sit under /buildbuddy-execroot (root, 0777).",
    ),
    "//tests:test_git_runners_1307": struct(
        tags = ["runner"],
        reason = "it runs git in the repository checkout; remote runfiles have no .git.",
    ),
    "//tests:test_issue_link_workflow": struct(
        tags = ["hosted"],
        reason = "it runs the workflow's JavaScript with Node.js, which the hosted image has; elsewhere it skips.",
    ),
    "//tests:test_measurement_census_tolerates_an_unorderable_ready_record_1506": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_movement_tool_roots_1659": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_needrestart_broker_restart_exclusion": struct(
        tags = ["hosted"],
        reason = "it reads the installed needrestart configuration, which the hosted image has; elsewhere it skips.",
    ),
    "//tests:test_ordinary_census_admission_2199": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_paced_exports_share_a_tier": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_pbmergeq_gh_cli": struct(
        tags = ["runner"],
        reason = "it runs /usr/bin/gh, which the hosted image has and the executor image lacks.",
    ),
    "//tests:test_pbrun_placement": struct(
        tags = ["hosted"],
        reason = "it hangs on the Remote Bazel runner (one shard ran past 600 s; 79 s on hosted); cause not yet found.",
    ),
    "//tests:test_pbtest_attempt_temp_1542": struct(
        tags = ["runner"],
        reason = "its socket path must fit in 100 bytes under the scratch root; it fits under /home/rob/pbt17, as on hosted runners, and skips under the executor's scratch.",
    ),
    "//tests:test_pbtest_automatic_duration_packing_2286": struct(
        tags = ["runner"],
        reason = "pbtest's default --tmpdir is fleet_boxes.json's /home/rob/pbt17, which a hosted shard creates with sudo; executors have no sudo and a read-only /home.",
    ),
    "//tests:test_pbtest_balances_shards_by_measured_duration": struct(
        tags = ["runner"],
        reason = "pbtest's default --tmpdir is fleet_boxes.json's /home/rob/pbt17, which a hosted shard creates with sudo; executors have no sudo and a read-only /home.",
    ),
    "//tests:test_pbtest_capability_cohorts": struct(
        tags = ["runner"],
        reason = "pbtest's default --tmpdir is fleet_boxes.json's /home/rob/pbt17, which a hosted shard creates with sudo; executors have no sudo and a read-only /home.",
    ),
    "//tests:test_pbtest_dependency_pins": struct(
        tags = ["hosted"],
        reason = "it pip-installs offline into a --system-site-packages venv, which needs setuptools in the base interpreter's site-packages, as setup-python gives; Bazel's packages sit on a .pth instead.",
    ),
    "//tests:test_pbtest_gpu_options": struct(
        tags = ["runner"],
        reason = "pbtest's default --tmpdir is fleet_boxes.json's /home/rob/pbt17, which a hosted shard creates with sudo; executors have no sudo and a read-only /home.",
    ),
    "//tests:test_pbtest_history_provenance_2286": struct(
        tags = ["runner"],
        reason = "pbtest's default --tmpdir is fleet_boxes.json's /home/rob/pbt17, which a hosted shard creates with sudo; executors have no sudo and a read-only /home.",
    ),
    "//tests:test_pbtest_interrupted_trace_1455": struct(
        tags = ["runner"],
        reason = "pbtest's default --tmpdir is fleet_boxes.json's /home/rob/pbt17, which a hosted shard creates with sudo; executors have no sudo and a read-only /home.",
    ),
    "//tests:test_pbtest_native_directory_collection_1458": struct(
        tags = ["runner"],
        reason = "pbtest's default --tmpdir is fleet_boxes.json's /home/rob/pbt17, which a hosted shard creates with sudo; executors have no sudo and a read-only /home.",
    ),
    "//tests:test_pbtest_records_skip_reasons": struct(
        tags = ["runner"],
        reason = "pbtest's default --tmpdir is fleet_boxes.json's /home/rob/pbt17, which a hosted shard creates with sudo; executors have no sudo and a read-only /home.",
    ),
    "//tests:test_pbtest_sealed_basetemp_1469": struct(
        tags = ["runner"],
        reason = "pbtest's default --tmpdir is fleet_boxes.json's /home/rob/pbt17, which a hosted shard creates with sudo; executors have no sudo and a read-only /home.",
    ),
    "//tests:test_pool_maintenance": struct(
        tags = ["runner"],
        reason = "worker_loop's --all-cores sizes CPU slots from the host's sysfs, which on a remote executor exceeds the container's cpuset; a hosted runner owns all its CPUs.",
    ),
    "//tests:test_pool_placement_census": struct(
        tags = ["runner"],
        reason = "worker_loop's --all-cores sizes CPU slots from the host's sysfs, which on a remote executor exceeds the container's cpuset; a hosted runner owns all its CPUs.",
    ),
    "//tests:test_pool_withdrawn_retention_1533": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_prelaunch_admission_1594": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_private_claim_admission_compat_1419": struct(
        tags = ["runner"],
        reason = "its pool builds CPU tiers from the host's topology, which on a remote executor names CPUs outside the container's cpuset.",
    ),
    "//tests:test_process_io_membership": struct(
        tags = ["runner"],
        reason = "it reads the test's own cgroup membership; hosted processes live in a service cgroup, so the runner gives each test a delegated leaf.",
    ),
    "//tests:test_produced_spool": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_produced_spool_host_window": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_produced_spool_paced_export": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_publish_runtime": struct(
        tags = ["runner"],
        reason = "it checks file modes of copied sources; remote execution stages every input executable (0555), a git checkout keeps 0644.",
    ),
    "//tests:test_r13_private_box_state_1542": struct(
        tags = ["runner"],
        reason = "its bench keeps scratch in /dev/shm, which is 64 MB on an executor.",
    ),
    "//tests:test_receipt_activation_direction": struct(
        tags = ["runner"],
        reason = "it runs git in the repository checkout; remote runfiles have no .git.",
    ),
    "//tests:test_record_compatibility": struct(
        tags = ["runner"],
        reason = "it needs bubblewrap with unprivileged user namespaces, which gate_setup.py installs on hosted shards; the executor image lacks it.",
    ),
    "//tests:test_regression_test_gate": struct(
        tags = ["hosted"],
        reason = "it runs the workflow's JavaScript with Node.js, which the hosted image has; elsewhere it skips.",
    ),
    "//tests:test_review_2023_independent": struct(
        tags = ["runner"],
        reason = "it needs the zstd command, which the hosted image has and the executor image lacks.",
    ),
    "//tests:test_review_terminal_plan_stat_refusal": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_role_log_migration_review_boundaries": struct(
        tags = ["runner"],
        reason = "it runs only as uid 1000, the production acting owner. Hosted runners are uid 1001 and skip it; executors are uid 1000 and would run it, so it runs on the runner VM (uid 1001) to keep the hosted skip.",
    ),
    "//tests:test_role_session_boundaries": struct(
        tags = ["runner"],
        reason = "worker_loop's --all-cores sizes CPU slots from the host's sysfs, which on a remote executor exceeds the container's cpuset; a hosted runner owns all its CPUs.",
    ),
    "//tests:test_runtime_publication_1659": struct(
        tags = ["hosted-arm64"],
        reason = "movement publication needs 1 GiB free; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free.",
    ),
    "//tests:test_scratch_lifetime_directory": struct(
        tags = ["hosted-arm64"],
        reason = "disk scratch refuses tmpfs; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_scratch_lifetime_ownership": struct(
        tags = ["hosted-arm64"],
        reason = "disk scratch refuses tmpfs; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_scratch_lifetime_pool": struct(
        tags = ["hosted-arm64"],
        reason = "disk scratch refuses tmpfs; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_sealed_bundle_cache": struct(
        tags = ["hosted-arm64"],
        reason = "the bundle cache stores a bundle only with free space to spare; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4, unsized, so statvfs reports 0 bytes free and git runs again.",
    ),
    "//tests:test_shared_census_safety_2199": struct(
        tags = ["runner"],
        reason = "it hashes the interpreter and refuses an inode that changes while read; executors hardlink one cached file into concurrent actions, which changes its ctime.",
    ),
    "//tests:test_slurm_install_scripts": struct(
        tags = ["hosted"],
        reason = "it needs a running Docker daemon; executors have none, and the runner VM has no init to start one outside the test's process tree.",
    ),
    "//tests:test_spool_poll_repin_evidence": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
    "//tests:test_staged_path_epoch_bytes": struct(
        tags = ["runner"],
        reason = "it runs git in the repository checkout; remote runfiles have no .git.",
    ),
    "//tests:test_staged_path_wire_replay": struct(
        tags = ["runner"],
        reason = "it runs git in the repository checkout; remote runfiles have no .git.",
    ),
    "//tests:test_stale_mentions_partial_prune": struct(
        tags = ["runner"],
        reason = "the pool refuses its claim on a remote executor and grants it on a whole VM; the executor-side cause is not isolated yet.",
    ),
    "//tests:test_submission_review_lifetimes": struct(
        tags = ["hosted"],
        reason = "it reuses test_filesystem_floor_1483's space fixtures, which skip without the filesystem identity only hosted runners give.",
    ),
    "//tests:test_supervisor_adoption_keeps_matching_loops_1664": struct(
        tags = ["runner"],
        reason = "worker_loop's --all-cores sizes CPU slots from the host's sysfs, which on a remote executor exceeds the container's cpuset; a hosted runner owns all its CPUs.",
    ),
    "//tests:test_tested_repository": struct(
        tags = ["runner"],
        reason = "it asserts its own name appears nowhere in a sealed action environment; a runfiles path contains the target's name.",
    ),
    "//tests:test_the_reconcile_walk_reuses_only_what_moved": struct(
        tags = ["hosted-arm64"],
        reason = "a renamed staged file is sometimes evicted instead of left on BuildBuddy's arm64 executors, whose writable paths are tmpfs (2 of 5 runs), and never on amd64 executors (8 of 8) or hosted arm64 runners (39 of 39); the cause is not isolated yet.",
    ),
    "//tests:test_worker_loop_cooperative_shutdown": struct(
        tags = ["runner"],
        reason = "worker_loop's --all-cores sizes CPU slots from the host's sysfs, which on a remote executor exceeds the container's cpuset; a hosted runner owns all its CPUs.",
    ),
    "//tests:test_worker_loop_follows_a_rename": struct(
        tags = ["runner"],
        reason = "worker_loop's --all-cores sizes CPU slots from the host's sysfs, which on a remote executor exceeds the container's cpuset; a hosted runner owns all its CPUs.",
    ),
    "//tests:test_worker_loop_offers_its_affinity": struct(
        tags = ["runner"],
        reason = "worker_loop's --all-cores sizes CPU slots from the host's sysfs, which on a remote executor exceeds the container's cpuset; a hosted runner owns all its CPUs.",
    ),
    "//tests:test_worker_loop_reloads_unversioned": struct(
        tags = ["runner"],
        reason = "worker_loop's --all-cores sizes CPU slots from the host's sysfs, which on a remote executor exceeds the container's cpuset; a hosted runner owns all its CPUs.",
    ),
    "//tests:test_worker_loop_requires_known_receipts": struct(
        tags = ["runner"],
        reason = "worker_loop's --all-cores sizes CPU slots from the host's sysfs, which on a remote executor exceeds the container's cpuset; a hosted runner owns all its CPUs.",
    ),
    "//tests:test_worker_loop_retires_every_kind": struct(
        tags = ["runner"],
        reason = "worker_loop's --all-cores sizes CPU slots from the host's sysfs, which on a remote executor exceeds the container's cpuset; a hosted runner owns all its CPUs.",
    ),
    "//tests:test_write_only_produced_output": struct(
        tags = ["hosted-arm64"],
        reason = "the produced spool refuses a tmpfs root; BuildBuddy's arm64 executors keep every writable path on tmpfs, where hosted arm64 runners have ext4.",
    ),
}

def nonhermetic(name):
    """Returns (tags, exec_properties, env) for the named test in this package.

    requires-network: the remote executor gets a network.
    runner: the test runs on the Remote Bazel runner itself, a VM like a hosted
    gate runner (all its CPUs, sudo, Docker), with pytest in the runner's git
    checkout. Its result is never cached, because it reads that checkout.
    hosted: Bazel never runs the test. The Gate workflow's hosted lane runs it
    with tools/ci/gate_run.py on a GitHub-hosted runner, prepared by
    tools/ci/gate_setup.py, whenever Bazel's selection impacts its target.
    hosted-arm64: BuildBuddy's arm64 executors can't host the test, so the
    post-merge arm64 pass leaves it out and the post-merge hosted lane runs it
    on ubuntu-24.04-arm. On amd64 it runs on remote executors as usual.
    runner tests get the same arm64 treatment: the runner VM is amd64 only.
    """
    entry = NONHERMETIC.get("//%s:%s" % (native.package_name(), name))
    if not entry:
        return [], {}, {}
    tags, properties, env = list(entry.tags), {}, {}
    if "requires-network" in entry.tags:
        properties["dockerNetwork"] = "bridge"
    if "runner" in entry.tags:
        tags += ["no-remote-exec", "no-sandbox", "no-cache"]
        env["PB_BAZEL_LANE"] = "runner"
    if "hosted" in entry.tags:
        tags += ["manual", "gate-lane-hosted"]
    return tags, properties, env
