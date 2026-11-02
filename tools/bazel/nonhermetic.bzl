"""Tests that cannot run hermetically, each with its reason.

The remote platform turns networking off and runs each test in a container
that has a few CPUs of a larger host, as an unprivileged user, on a runfiles
tree with every input executable and no .git. A test listed here gets back
what its tags name. Keep this table short and the
reasons true; the PR that adds an entry says why the test can't use a declared
input instead.
"""

NONHERMETIC = {
    "//tests:test_action_resource_profile": struct(
        tags = ["runner"],
        reason = "it reads the test's own cgroup membership; hosted processes live in a service cgroup, so the runner gives each test a delegated leaf.",
    ),
    "//tests:test_attempt_log_retention_pacing": struct(
        tags = ["runner"],
        reason = "it needs the zstd command, which the hosted image has and the executor image lacks.",
    ),
    "//tests:test_broker_bridge_publication_2229": struct(
        tags = ["runner"],
        reason = "the installer refuses a root-owned, world-writable ancestor; remote runfiles sit under /buildbuddy-execroot (root, 0777).",
    ),
    "//tests:test_checkout_recovery_1465": struct(
        tags = ["runner"],
        reason = "it calls io_uring_setup, which the executor's seccomp profile refuses (ENOSYS).",
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
    "//tests:test_filesystem_floor_1483": struct(
        tags = ["runner"],
        reason = "it needs a non-loopback interface address; remote executors run without a network.",
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
    "//tests:test_pbmergeq_gh_cli": struct(
        tags = ["runner"],
        reason = "it runs /usr/bin/gh, which the hosted image has and the executor image lacks.",
    ),
    "//tests:test_pbrun_placement": struct(
        tags = ["hosted"],
        reason = "it hangs on the Remote Bazel runner (one shard ran past 600 s; 79 s on hosted); cause not yet found.",
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
    "//tests:test_private_claim_admission_compat_1419": struct(
        tags = ["runner"],
        reason = "its pool builds CPU tiers from the host's topology, which on a remote executor names CPUs outside the container's cpuset.",
    ),
    "//tests:test_process_io_membership": struct(
        tags = ["runner"],
        reason = "it reads the test's own cgroup membership; hosted processes live in a service cgroup, so the runner gives each test a delegated leaf.",
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
    "//tests:test_review_2023_independent": struct(
        tags = ["runner"],
        reason = "it needs the zstd command, which the hosted image has and the executor image lacks.",
    ),
    "//tests:test_role_session_boundaries": struct(
        tags = ["runner"],
        reason = "worker_loop's --all-cores sizes CPU slots from the host's sysfs, which on a remote executor exceeds the container's cpuset; a hosted runner owns all its CPUs.",
    ),
    "//tests:test_shared_census_safety_2199": struct(
        tags = ["runner"],
        reason = "it hashes the interpreter and refuses an inode that changes while read; executors hardlink one cached file into concurrent actions, which changes its ctime.",
    ),
    "//tests:test_slurm_install_scripts": struct(
        tags = ["hosted"],
        reason = "it needs a running Docker daemon; executors have none, and the runner VM has no init to start one outside the test's process tree.",
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
    "//tests:test_supervisor_adoption_keeps_matching_loops_1664": struct(
        tags = ["runner"],
        reason = "worker_loop's --all-cores sizes CPU slots from the host's sysfs, which on a remote executor exceeds the container's cpuset; a hosted runner owns all its CPUs.",
    ),
    "//tests:test_tested_repository": struct(
        tags = ["runner"],
        reason = "it asserts its own name appears nowhere in a sealed action environment; a runfiles path contains the target's name.",
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
