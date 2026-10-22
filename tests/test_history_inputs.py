"""Collected tests must get retained source from the committed tree (#1999)."""
from __future__ import annotations

import ast
import re
import shlex
from pathlib import Path
from itertools import product

from prismabuild.digest_primitives import raw_sha256

import pytest

import history_inputs

HISTORY_TOOLS = {
    "compat/support/compat_sources.py": "Maintenance capture deliberately resolves historical compatibility sources.",
    "compat/support/rebind_source_reviews.py": "Maintenance rebinding compares reviewed sources across explicit revisions.",
    "compat/support/record_compat.py": "Compatibility capture records the selected historical source identity.",
    "c07_review_audit.py": "Standalone source audit compares the reviewed C07 baseline with current sources.",
    "lifecycle_source_audit.py": "Standalone lifecycle audit compares explicit historical source trees.",
}
HISTORY_VERBS = {"show", "archive", "cat-file", "ls-tree", "rev-list", "log", "diff"}


# Dynamic non-Git launchers and generated-repository proofs already in this tree.
# These are byte-pinned exceptions, not exemptions for future edits to a file.
# A changed module must be reviewed again; do not refresh a pin to silence a finding.
HISTORY_PROCESS_EXCEPTIONS = {
    'c26_native_support.py': (
        ('09d0cbc39d984276ffeac914434462efe57a01c2674b239b462d02b16351550a',),
        ((118, '?'),),
        'Runs generated native pytest fixture commands.'),
    'capture_proof_runtime.py': (
        ('6814bbc0bfde1e095510d3dff11765155fc331292126eb682359db8cc836249d',),
        ((254, '?'), (925, '?'), (939, '?'), (963, '?'), (1013, '?')),
        'Captures ldconfig and Docker loader diagnostics; the dynamic commands inspect runtime files.'),
    'compat/support/audit_compat_actions.py': (
        ('14f0d10be1e3263e562a65424b0857c7468c64bf86a8442a214dfb6e71e8e6d1',),
        ((17, 'bundle'), (19, 'show'), (24, 'diff-tree'), (123, 'bundle')),
        'Verifies commit parent/tree identities from a supplied sealed snapshot bundle in a fresh bare repository. Also lists and unbundles those supplied objects.'),
    'compat/support/compat_namespace_probe.py': (
        ('1d8eb29af8506f374d12649953aa82673c8cefc57c731e32008454a0a36a4e35',),
        ((18, '?'), (60, '?')),
        'Executes bwrap and its version probe for the private proc namespace.'),
    'compat/support/compat_recovery_adapters.py': (
        ('ae36ab6c49fe82d556918acdfaccd5566014eccebce75af4db900bb2bdd884b6',),
        ((52, 'clone'), (54, 'symbolic-ref'), (55, 'update-ref')),
        'Restores refs from a supplied snapshot bundle in a temporary recovery repository.'),
    'conftest.py': (
        ('26f5e930350440babd5aef607d8e7d1a588e72af1b6802b97e1bb2d0e746c424',),
        ((929, '?'),),
        'Executes the bounded filesystem reachability probe in a Python child.'),
    'gpu_memory_scope_probe.py': (
        ('bb4edeb8d8132d9ffd8f54fa6d6233c2f1bac2f5d85b3b98ea298f7d2ba75c95',),
        ((47, '?'),),
        'Runs the Python memory-scope probe through its GPU containment adapter.'),
    'proof_container_runtime.py': (
        ('ac9f166e1be28db50c34ea21eaa191ca18e3e017feaa9da01ccc44b558d578a3',),
        ((2042, '?'),),
        'Executes the captured runtime interpreter under the declared reader credentials.'),
    'run_proof_container.py': (
        ('ed7535f3a980131e2f53336e4b7da8bc2c3b0b01bba0d646c9964beb2d8f9d8e',),
        ((132, '?'), (187, '?')),
        'Runs the proof Docker image and its startup diagnostics.'),
    'test_a_movers_retries_and_determinism_are_its_own.py': (
        ('69c333cd9f4ef8e456440284a3842dada193c0cbbaf468415e91243b1196ecec',),
        ((250, '?'),),
        'Executes the generated mover proof Python program.'),
    'test_a_resumed_decomposition_publishes_only_what_is_missing.py': (
        ('9b0c9f7c1635c491a0ec16ebd6e43eca6ffa04cf31f098d07668849643ded1ab',),
        ((320, 'clone'),),
        'Clones the generated fixture source repository for the resumed decomposition proof.'),
    'test_adaptive_cpu_local_state.py': (
        ('1f70bd481f9cdcbd7bb26473580235a103bc4f12d9d93a5256bf1666cca9e9e8',),
        ((96, '?'), (302, '?')),
        'Calls the captured subprocess runner while exercising adaptive CPU fixture state.'),
    'test_canary_leg3_staged_read.py': (
        ('3572b7eb53a8e5f86647bc94031854c3a7b5da20fc1e77c7969b04da0bdd4ce8',),
        ((160, '?'),),
        'Compiles the C forbidden-origin tripwire into a shared library.'),
    'test_capture_argv.py': (
        ('73d617ea59a63683c6fa737703394bd25eb2b4c3e9cf0adefa1ff3773c14b01f',),
        ((22, '?'),),
        'Exercises argument boundaries through the sealed output-capture launcher.'),
    'test_checkout_identity_is_byte_derived.py': (
        ('1fe2da3d5765b06884604bf7a3a5014be57f70c4caf7dd63c5cb432705fe8167',),
        ((68, '?'), (291, 'worktree'), (294, 'rev-parse')),
        'Replays the legacy dirty diff against a temporary fixture repository. Its linked-worktree fixture also queries the index path.'),
    'test_checkout_recovery_1465.py': (
        ('462cfcb6de69b97c6a12ab3f16039154d3e037e8a865d7acbd4138b7a041ab8f',),
        ((1030, '?'), (1446, '?')),
        'Compresses a fixture bank with zstd and compiles the fixture C shared library.'),
    'test_client_proof_prior_trust.py': (
        ('85cacfa70c0b39300cc58b815453adcd3f1dc839300635ac0dbdc7594882193d',),
        ((72, '?'),),
        'Runs the resolved Python interpreter under the fixture reader credentials.'),
    'test_compat_capture_intake.py': (
        ('fcee12dcd77962f9ae22ee674ca39a01e1da8ae7fb1cec675e3f3e318a71752c',),
        ((47, '?'), (107, '?')),
        'Executes the compatibility capture Python tool against fixture stores.'),
    'test_compat_namespace_preflight.py': (
        ('dce361fcd316a2db3dfd7e2fef1ebca2da73c0730687ce6bb59dcfc5637d8d83',),
        ((59, '?'),),
        'Runs nested pytest to exercise compatibility namespace collection refusal.'),
    'test_deferred_action_edges.py': (
        ('a6b427774258f7d61d5df66f99baccef52a15435289469fcac8d72decff1e5cf',),
        ((323, '?'), (328, '?')),
        'Runs the sealed fixture action commands to verify dependency edges.'),
    'test_diag811_realpack_descriptor.py': (
        ('9f08a8694100d016d7e934c6e16105d2acc7b7334b02d3a7fe4e787f41d519ba',),
        ((31, '?'), (63, 'bundle'), (109, 'checkout')),
        'The adapter forwards stringified arguments to Git for a generated temporary pack fixture.'),
    'test_digest_primitives_1547.py': (
        ('4adf207337a7609ef3c478ebe6286f1b5b6bd3b6078b837cdd1f173ad900b913', '7542903516b4f03c0a1c1aae4cd885580170a1bcc9d693598b80981d48da1872'),
        ((66, '?'),),
        'Runs the isolated Python pytest shard to verify shipped digest primitives.'),
    'test_docker_removal_in_progress_1403.py': (
        ('81d274bdc67a611b1b63d064da839aa7a4275f170b664e2f6410dadcac4b21aa',),
        ((43, '?'),),
        'Calls the captured subprocess runner for fixture Docker removal.'),
    'test_docker_shim_global_options.py': (
        ('e83d85fc76e3bf66dda9f2cfba462137dee586a838b421ad3ec3372850ca2fc8',),
        ((103, '?'),),
        'Runs the fixture Docker shim, optionally inside taskset.'),
    'test_docker_shim_relays_termination_1599.py': (
        ('8862e8b891b37bf43a11c56bb1434a4cbd026d22b0e24ce6ae7979a9ea5600b3',),
        ((250, '?'),),
        'Runs the fixture Docker shim to exercise signal relay.'),
    'test_exact_role_log_metadata_migration.py': (
        ('c5f13d91524ffcd354f8f23ba6a419ea49c0338d528c6abad43cce5489e7444d',),
        ((427, '?'), (431, '?')),
        'Duplicates the fixture child command to test process identity reuse.'),
    'test_fleet_membership_busy_resign.py': (
        ('d091ca6e953ae1723ce74934ed134f07165f1ad38877497053d6d9ceb4518e19',),
        ((1747, '?'),),
        'Runs the generated fixture process to test busy membership resignation.'),
    'test_fleet_membership_join_resign.py': (
        ('c9d33a42044ae7c7ec06eff867f7151c45412c1866a678d0af6ccfca512545fe',),
        ((541, '?'), (631, '?')),
        'Runs fixture membership commands and their captured subprocess runner.'),
    'test_install_ram_mount_unit.py': (
        ('5754d7342a5c60d9883c27cfcdc18953972886b32f3e759df2f4d81964d021b8',),
        ((612, '?'),),
        'Runs systemd-analyze against generated fixture units.'),
    'test_issue_link_workflow.py': (
        ('245e4a16aa8e3980fb37912a16d7c30e4a40e0fd2a8a1e0b6f4e724ea3dcace4',),
        ((52, '?'),),
        'Runs the Node workflow harness against issue-link scenarios.'),
    'test_live_store_guard.py': (
        ('26bafc6ef5ce8021b94a6fbb8fbba018e2943d20824eda00999dd038cf79f623',),
        ((187, '?'),),
        'Runs nested pytest in a generated fixture suite to verify the live-store guard.'),
    'test_metrics_supervisor_retirement.py': (
        ('0655625e8edeb40a88b334e77c87a82b9ec57fd5886b618d082488c548fd5b10',),
        ((133, '?'),),
        'Forwards the fixture process launcher to the captured Popen implementation.'),
    'test_mount_latency.py': (
        ('5f4077d7b17bbb51a168e8f4b927cbec046ef7c990e32f90995360cfe2684bb7',),
        ((107, '?'), (140, '?')),
        'Runs the fixture Netdata plugin directly or through Python.'),
    'test_nfs_readahead.py': (
        ('f5acaa45850a0385214981950149b45e72cc66671782e148ec51ca396e1e7de0',),
        ((168, '?'),),
        'Runs systemd-analyze against the generated readahead fixture unit.'),
    'test_ordinary_role_import_boundary.py': (
        ('f5a54cd7fa0d8b186b290f4a643c9508cba40ef9f1fb9955ea0e25e89a4b6d0c',),
        ((34, '?'), (143, '?')),
        'Runs the selected Python interpreter with isolated import bootstrap.'),
    'test_pbcanary_legs12.py': (
        ('94b290f1cc7608471b46de4f38097f5843641f0e79d9ef8a0d85e0e40939ff11',),
        ((29, '?'),),
        'Executes the canary fixture specification command.'),
    'test_pbmcp_read_only.py': (
        ('43c031bb012d95faae7ea63867d68d3b400e71fdf61e5b782cfc5b8e18f97cb9',),
        ((347, '?'),),
        'Runs a fixture subprocess while proving MCP read-only behavior.'),
    'test_pbmergeq.py': (
        ('302c502150c2ebf3a41923dacd66848ab5ffff9d3fdf4044b36b9ba981b69252',),
        ((154, 'checkout'), (161, 'update-ref'), (162, 'checkout'), (177, 'rev-parse'), (359, 'checkout'), (363, 'checkout'), (364, 'rev-parse'), (365, 'rev-parse'), (388, 'rev-parse'), (405, 'rev-parse'), (409, '?'), (411, 'update-ref'), (513, '?'), (551, '?'), (593, 'checkout'), (598, 'update-ref'), (599, 'checkout'), (621, 'checkout'), (626, 'update-ref'), (627, 'checkout'), (677, 'checkout')),
        'Builds fake-origin branches and pull refs in temporary repositories and runs the Python pytest shard.'),
    'test_pbmergeq_review_regressions.py': (
        ('52a23ceb288e7904c471057d2dd156eaf79fd3b898025039c5c3ec3e058d82dd',),
        ((135, '?'),),
        'Runs the generated review regression fixture command.'),
    'test_pbmetrics_kept_reads.py': (
        ('c907ad8e6f4ba3ee03e839ccce4c99ec45a2025b63cc9ab805cdd5273c93b2f5',),
        ((738, '?'),),
        'Starts the fixture metrics Python process with its input pipe.'),
    'test_pbrun_bundle_determinism.py': (
        ('c93a72b575e150062e35524f3f4674184fe6a8c05349f7996fc6d8e6bb348b36',),
        ((146, '?'), (215, 'branch'), (222, 'bundle'), (236, 'fetch'), (240, 'checkout'), (244, 'bundle')),
        'Constructs, fetches and checks out generated temporary Git bundles to compare determinism.'),
    'test_pbrun_container_lifecycle.py': (
        ('09f04f675a96721678e28df881e0fae51d88a2fcab4ffa2884600bfe977c7fc0',),
        ((111, '?'), (164, '?'), (190, '?')),
        'Runs the fixture Docker shim lifecycle commands.'),
    'test_pbrun_exclude_reaches_worktrees.py': (
        ('41a0eaa801a2611f3f9d8601a1162d0d6a3bc972d412932c82634403e6fb7f00',),
        ((59, 'worktree'), (82, 'worktree'), (173, '?')),
        'Creates temporary linked worktrees and invokes the captured fixture subprocess runner.'),
    'test_pbrun_placement.py': (
        ('082e08d1b4815cb3bce5b496600154dadaa63f2ed8101494569e4966385e1e0d',),
        ((725, '?'), (748, '?'), (795, 'fetch'), (802, 'checkout'), (818, 'worktree'), (822, 'rev-parse'), (840, 'fetch'), (847, 'checkout'), (944, 'fetch'), (951, 'checkout'), (975, 'submodule'), (1057, 'rev-parse'), (1058, 'diff'), (1088, 'rev-parse'), (1089, 'diff'), (1183, 'clone'), (1250, 'clone'), (2505, '?'), (2524, '?'), (2544, '?'), (2572, '?'), (2625, '?'), (2651, '?'), (2768, '?'), (2904, '?'), (2929, '?')),
        'Compares generated source/snapshot repository ancestry and runs sealed fixture commands; no retained project commit is required.'),
    'test_pbrun_sealed_environment.py': (
        ('12c420c946f7722f94ffe5a94166a90252564b922a5826b6eed71e4f03c03803',),
        ((36, '?'),),
        'Executes the sealed Bash fixture to check login-shell state.'),
    'test_pbrun_stamp_overlay.py': (
        ('d93f3dff06e4d94ddb40d69b11fad261eebd84a31d6c3d7b2fc9393581edcc23',),
        ((43, 'clone'),),
        'Clones the generated fixture snapshot to inspect its stamp overlay.'),
    'test_pbstatus.py': (
        ('cf5cbcd146aed68a90473f41af997b0f45f9fc21e81a460a13c8278db4dc97d3',),
        ((704, '?'),),
        'Runs the Python status CLI with fixture fleet arguments.'),
    'test_pbtest.py': (
        ('b9bf46d846406a5585359fe8da34eb05f3bc8197a6ebad698e7449f06ef2bb51',),
        ((266, '?'), (314, '?'), (326, '?')),
        'Executes the Python pytest shard payload against the temporary checkout.'),
    'test_pbtest_attempt_temp_expiry.py': (
        ('51b093a5ac54430d6ebe6e8cf603656ab4688d197114a39159dc9883376af8f4',),
        ((82, '?'), (89, '?')),
        'Executes Python pytest shards to verify attempt temporary-directory cleanup.'),
    'test_pbtest_balances_shards_by_measured_duration.py': (
        ('ced73f6e4d0ba7f03618d9903b2f2af84f329ed4ff9d33eb6aaeaa4387ea3cab',),
        ((73, '?'), (75, '?')),
        'Commits generated fixture tests through the captured subprocess runner.'),
    'test_pbtest_bound_failure_kinds.py': (
        ('f46fa0029a388b1867bd0e1b3177e61b45fdc1c31927263a271dc66e9a64da22',),
        ((61, '?'),),
        'Executes a Python pytest shard to verify bounded failures.'),
    'test_pbtest_capability_cohorts.py': (
        ('6a70979e5cc54fb77054e160869794c6cc336ea9027a5b0616792aa05599db57',),
        ((121, '?'), (158, '?')),
        'Invokes fixture shard commands through the captured subprocess runner.'),
    'test_pbtest_collection_owner_shard.py': (
        ('1e932cc07b361cd977c047011b7d26c404315bebb99fbb35bf90f6c818555e79',),
        ((51, '?'),),
        'Executes a Python pytest shard to verify collection ownership.'),
    'test_pbtest_dependency_pins.py': (
        ('5aff3a2da4387f6767d9050c659435c263023cebe208297e711b28d2f1743caa',),
        ((74, '?'),),
        'Commits the generated local dependency fixture with the captured subprocess runner.'),
    'test_pbtest_digest_owner_shard_1553.py': (
        ('39aeab8da9713ec28725eb61cb1316e364cc590d61bf035a8b11705e25122a34',),
        ((44, '?'),),
        'Executes a Python pytest shard with the shipped digest owner.'),
    'test_pbtest_exit_completion_1530.py': (
        ('ae6ae3ae66bee0c2f37a959609b7fd507bb2a91db59c733caa141e2ea096f97c',),
        ((81, '?'),),
        'Executes a Python pytest shard to verify process-exit completion.'),
    'test_pbtest_gpu_options.py': (
        ('0181ade5ec8ea519ecd93acb16f55a8730260c47b21056bbb4c9ec554dfe2cd3',),
        ((100, '?'),),
        'Invokes the captured fixture shard command to inspect GPU options.'),
    'test_pbtest_honours_collect_ignore_1304.py': (
        ('a636cb2fe5e0dc996b14068c44f7681e2d1375c9822007d2344649f508398562',),
        ((25, '?'),),
        'Executes a Python pytest shard to verify collection ignores.'),
    'test_pbtest_identifies_its_checkout_once.py': (
        ('2f21b28cf7128635235f467eabc3ab7ef8bfadcde8b2d693e9527747809a5bdb',),
        ((123, '?'),),
        'Invokes the captured fixture runner to observe checkout identification.'),
    'test_pbtest_interrupted_trace_1455.py': (
        ('6fbdb1b933cf8464164345bde25668b759c5dc1c3f93b87d02da47e0f82cd050',),
        ((39, '?'), (180, '?'), (183, '?')),
        'Executes a Python pytest shard to verify interrupted trace handling.'),
    'test_pbtest_native_directory_collection_1458.py': (
        ('408cfa076dc03a2d142a683842a23f143875f2233ac02eb4be3ffff23f015075',),
        ((70, '?'), (82, '?'), (85, '?')),
        'Executes a Python pytest shard for native directory collection.'),
    'test_pbtest_reconciles_outcomes.py': (
        ('dd92724ed7c7799d8289a65c786797b3d6aea646356886952cf076228f856e1c',),
        ((92, '?'), (94, '?'), (250, '?')),
        'Commits fixture tests and runs fixture shards through the captured subprocess runner.'),
    'test_pbtest_records_skip_reasons.py': (
        ('b07474f5444e81cea7bff6ff0b29dc7b898e23f7f4ba09d086d3cf3dc4cb2264',),
        ((123, '?'), (126, '?'), (195, '?')),
        'Commits fixture tests through the captured subprocess runner.'),
    'test_pbtest_reports_whether_pytest_ran.py': (
        ('a7a7968e6fd3c734a40a8952297e7e69a189d46b69ab8a550dc08384b2f6422f',),
        ((76, '?'),),
        'Commits the generated fixture tests through the captured subprocess runner.'),
    'test_pbtest_retries_an_offer_discovery_timeout.py': (
        ('65c426ad9c827e94f881d0504ab4031ae04ad9abcb02dab08223acfb10c3e30a',),
        ((123, '?'),),
        'Commits the generated retry fixture through the captured subprocess runner.'),
    'test_pbtest_sealed_basetemp_1469.py': (
        ('7b4c39051f8bda57b76d7030762bf0c02b0e5b77db071fbc058c4078eda1501a',),
        ((116, '?'), (337, '?'), (411, '?'), (462, '?')),
        'Executes Python pytest shards against fixture basetemp roots.'),
    'test_pbtest_streams_pbrun_lines.py': (
        ('db0492d12ae35abaa18ce0a94d715fd1886fde5bf56420ae899dca779e099dad',),
        ((90, '?'), (155, '?')),
        'Runs the fixture shard via the captured subprocess runner to inspect streaming.'),
    'test_pbtest_tmp_retention_option_1535.py': (
        ('04f67fa0fd603dea47a949ad36864247150dfbaad908ad58d198673cc2c2798c',),
        ((101, '?'),),
        'Executes a Python pytest shard to verify scratch retention.'),
    'test_pbtest_trace_pinned_procfs_reader_1550.py': (
        ('532a296f5fbb07d0d1c27aecadf4f4f7f1c1dc7fa8de4da1bb92d31629f79240',),
        ((58, '?'),),
        'Executes a Python pytest shard with the pinned procfs reader.'),
    'test_pbtest_verdict_c26_vectors.py': (
        ('89fcfca64e6d08dc9cacbf9315204f4422912de311addc8e9bea27f52ba4b3e5',),
        ((18, '?'),),
        'Executes a Python pytest shard against verdict fixture vectors.'),
    'test_pbtest_xdist_shared_skip_1220.py': (
        ('039f435fc10fcf97120c23bcb521af0781a8d9308bc0ca1a91d38376cae5c2ea',),
        ((39, '?'),),
        'Executes a Python pytest shard to verify shared xdist skips.'),
    'test_physical_entry_bootstrap.py': (
        ('9d1900c67568beec0394b84f1e543e06aa44e5e795cbc072f2aeb6c0b08dabd0',),
        ((49, '?'),),
        'Forwards Git setup arguments for temporary physical-entry repositories.'),
    'test_pool.py': (
        ('f7e67f5c7d7cda70d1ab30555a03d4051de6b67442ff2833ec9ed74641f52a60',),
        ((432, 'clone'), (455, 'bundle')),
        'Clones and bundles a generated temporary repository for pool snapshot fixtures.'),
    'test_pool_broker_disconnect.py': (
        ('2ff010787a8b86a58a56226212f672b7458146a71e5c793c1fec377d3e3c3f81',),
        ((74, '?'),),
        'Executes the fixture broker request with inherited pipe descriptors.'),
    'test_pool_withdrawn_retention_1533.py': (
        ('a137b84f0518a4e29f808cb4f6e15d2250c8110c93bd3ca90ed604f6d60f11f3',),
        ((136, '?'),),
        'Runs the fixture retention command through the captured subprocess runner.'),
    'test_progress_declared_allowances.py': (
        ('95ac8106d6ce25b5e5acf9bb64d5f2bd350dcaf5eb64f7b44d8bed200bf6563e',),
        ((72, '?'),),
        'Runs the generated progress fixture command.'),
    'test_progress_reporting_is_reachable_from_any_action.py': (
        ('4dd0bff7efcc1f1cc20dec9b65bb13795f8979e9093169f9907d10db57149e90',),
        ((396, '?'), (418, '?')),
        'Runs progress fixture subprocesses through captured runners.'),
    'test_receipt_reader_root.py': (
        ('519d18cae10f7ab495dbb6c50cf81d2dcf5bd6f225fb559a5f1ae9c42e77e3a5',),
        ((229, '?'),),
        'Runs the fixture Python reader under its declared credentials.'),
    'test_record_compatibility_controller.py': (
        ('25c7c05ed80799d005ef3dee5a21b476005fefd601d775185ce8dc62ea3c0fba',),
        ((72, '?'), (91, '?')),
        'Forwards archival Python commands to the captured runner for environment/provenance controls.'),
    'test_sample_profile.py': (
        ('cb0c0d0ecf4481fab80fdb8e2216b9ad646f6de27365fb6ff4941f505a24d5b3',),
        ((1085, '?'),),
        'Starts the sampled fixture command through the profiler launcher.'),
    'test_sealed_output_capture.py': (
        ('efd620728e99e857d7b8c5cb2d33355871fa5a57d03f26c13a08e59553bf09c2',),
        ((35, '?'), (82, '?'), (96, '?'), (107, '?')),
        'Executes capture/tee fixture commands to verify output and exit propagation.'),
    'test_slurm_lane.py': (
        ('a937c768373030e9c1a15581b46b25a3bd4f540749a8745486cb5b58dc666087',),
        ((392, 'clone'), (403, 'bundle')),
        'Clones and bundles a generated temporary repository for SLURM fixtures.'),
    'test_snapshot_ignores_personal_git_excludes.py': (
        ('cd66b2bbbee2b49cde32ed8f1d22c94388c0796d5322f3a826a2a6ffd345212d',),
        ((84, 'fetch'), (88, 'checkout'), (91, 'rev-parse')),
        'Fetches the generated fixture bundle and compares its current tree identity.'),
    'test_snapshot_refuses_a_sparse_checkout.py': (
        ('4a835d9c64492ec2b18da3d0ef203e432ac853f948e5bd82bda96ef780c4e82f',),
        ((70, 'sparse-checkout'), (88, 'update-index')),
        'Configures sparse/index state in the generated temporary source repository.'),
    'test_snapshot_staged_ignored_paths.py': (
        ('b6e39486349d8382787e10487c40cbae3145eb65162b89a78328ce82624b8387',),
        ((58, 'fetch'), (62, 'checkout')),
        'Fetches and checks out the generated fixture snapshot bundle.'),
    'test_snapshot_symlink_containment.py': (
        ('572807c05a18ef69cd504ca077aa13b787452e4bf12ea28b5e536316be8a5398',),
        ((147, 'bundle'),),
        'Bundles the generated fixture commit for symlink containment checks.'),
    'test_submission_git_deadline_1888.py': (
        ('eecafbc2288d5d1339d619000604711368e59f15631d46980d0212fa5e9784cd',),
        ((44, '?'), (72, '?')),
        'Calls the captured fixture Git runner while observing preparation deadlines.'),
    'test_tested_repository.py': (
        ('d1e5b89387487116ce578d2284edf62043125c2a173514ede64204ee68b7efef',),
        ((269, 'worktree'), (292, 'clone'), (297, 'worktree')),
        'Creates temporary clones and linked worktrees to test repository identity.'),
    'test_worker_signal_ownership.py': (
        ('48dcbb8b8688dbc1e20e937b249cb49e5d4285f02b2a6e931d32a2a83c7b10b4',),
        ((24, '?'),),
        'Starts the fixture child with SIGINT ignored to verify signal ownership.'),
}



class _HistoryScope:
    def __init__(self, parent=None):
        self.parent = parent
        self.bindings = {}

    def bind(self, name, value):
        self.bindings.setdefault(name, []).append(value)

    def lookup(self, name):
        scope = self
        while scope is not None:
            if name in scope.bindings:
                values = scope.bindings[name]
                return scope, values[0] if len(values) == 1 else None
            scope = scope.parent
        return self, None


class _HistoryBindings(ast.NodeVisitor):
    """Keep lexical owners separate, including parameters and rebinding."""

    def __init__(self, tree):
        self.scope = _HistoryScope()
        self.root = self.scope
        self.scopes = {}
        self.owners = {}
        self.functions = {}
        self.visit(tree)

    def visit(self, node):
        self.scopes[node] = self.scope
        return super().visit(node)

    def visit_Name(self, node):
        if isinstance(node.ctx, (ast.Store, ast.Del)):
            self.scope.bind(node.id, None)

    def visit_Assign(self, node):
        self.visit(node.value)
        for target in node.targets:
            if isinstance(target, ast.Name):
                self.scope.bind(target.id, node.value)
                self.scopes[target] = self.scope
            else:
                self.visit(target)

    def visit_AnnAssign(self, node):
        if isinstance(node.target, ast.Name):
            self.scope.bind(node.target.id, node.value)
        else:
            self.visit(node.target)
        if node.value is not None:
            self.visit(node.value)

    def visit_Subscript(self, node):
        if isinstance(node.ctx, (ast.Store, ast.Del)) and isinstance(node.value, ast.Name):
            self.scope.bind(node.value.id, None)
        self.generic_visit(node)

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
            if node.func.attr in {'append', 'extend', 'insert', 'pop', 'remove', 'clear', 'sort', 'reverse'}:
                self.scope.bind(node.func.value.id, None)
        self.generic_visit(node)

    def visit_ExceptHandler(self, node):
        if node.name:
            self.scope.bind(node.name, None)
        self.generic_visit(node)

    def visit_MatchAs(self, node):
        if node.name:
            self.scope.bind(node.name, None)
        self.generic_visit(node)

    def visit_Import(self, node):
        for alias in node.names:
            self.scope.bind(alias.asname or alias.name.split('.')[0], alias.name)

    def visit_ImportFrom(self, node):
        for alias in node.names:
            self.scope.bind(alias.asname or alias.name, f"{node.module}.{alias.name}")

    def visit_FunctionDef(self, node):
        self.scope.bind(node.name, node)
        self.functions[node] = self.scope
        for value in [*node.decorator_list, *node.args.defaults,
                      *(v for v in node.args.kw_defaults if v is not None)]:
            self.visit(value)
        parent = self.scope
        # A method does not close over its class namespace.
        self.scope = _HistoryScope(parent.parent if self.owners.get(parent) == 'class' else parent)
        self.owners[self.scope] = node
        for arg in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs,
                    *([node.args.vararg] if node.args.vararg else []),
                    *([node.args.kwarg] if node.args.kwarg else [])]:
            self.scope.bind(arg.arg, None)
        for child in node.body:
            self.visit(child)
        self.scope = parent

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_ClassDef(self, node):
        self.scope.bind(node.name, node)
        for child in [*node.decorator_list, *node.bases, *node.keywords]:
            self.visit(child)
        parent = self.scope
        self.scope = _HistoryScope(parent)
        self.owners[self.scope] = 'class'
        for child in node.body:
            self.visit(child)
        self.scope = parent

    def visit_Lambda(self, node):
        parent = self.scope
        self.scope = _HistoryScope(parent)
        for arg in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]:
            self.scope.bind(arg.arg, None)
        self.visit(node.body)
        self.scope = parent

    def visit_Global(self, node):
        for name in node.names:
            self.scope.bind(name, None)
            self.root.bind(name, None)

    visit_Nonlocal = visit_Global


class _HistoryGuard:
    LAUNCHERS = {'run', 'call', 'check_call', 'check_output', 'Popen',
                 'system', 'popen', 'execl', 'execle', 'execlp', 'execlpe',
                 'execv', 'execve', 'execvp', 'execvpe', 'posix_spawn', 'posix_spawnp'}
    WRAPPERS = {'git', '_git', '_git_entry', 'lifecycle_audit_git'}
    # Known verbs locate the command after a wrapper's optional repository.
    # inspect() separately proves their operands; recognition is not permission.
    SETUP_VERBS = {'init', 'config', 'add', 'commit', 'status', 'clone', 'fetch',
                   'checkout', 'switch', 'branch', 'update-ref', 'symbolic-ref',
                   'remote', 'reset', 'clean', 'rm', 'mv', 'tag', 'bundle',
                   'ls-files', 'check-ignore', 'check-attr', 'hash-object',
                   'write-tree', 'commit-tree', 'update-index', 'read-tree',
                   'merge-base', 'rev-parse', 'worktree', 'submodule', 'sparse-checkout',
                   'index-pack', 'gc', 'count-objects'}
    VALUE_OPTIONS = {'-n', '--max-count', '--format', '--pretty', '--output', '--prefix'}
    FLAG_OPTIONS = {'-p', '-t', '-e', '-s', '-r', '-z', '--name-only', '--name-status',
                    '--stat', '--numstat', '--shortstat', '--raw', '--quiet',
                    '--exit-code', '--no-patch', '--oneline', '--no-color',
                    '--count', '--full-tree', '--abbrev-commit', '--no-renames', '--cached', '--staged'}

    def __init__(self, source):
        self.tree = ast.parse(source)
        self.lexical = _HistoryBindings(self.tree)
        self.heads = {}
        self.overrides = {}
        self.parents = {child: node for node in ast.walk(self.tree) for child in ast.iter_child_nodes(node)}

    def canonical(self, node, seen=frozenset()):
        if node in seen:
            return ''
        seen = seen | {node}
        if isinstance(node, ast.Name):
            _, value = self.lexical.scopes.get(node, self.lexical.root).lookup(node.id)
            if isinstance(value, (ast.Name, ast.Attribute)):
                return self.canonical(value, seen)
            if isinstance(value, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.id not in self.WRAPPERS:
                return '<local>.' + node.id
            return value if isinstance(value, str) else node.id
        if isinstance(node, ast.Attribute):
            return self.canonical(node.value, seen) + '.' + node.attr
        return ''

    def binding(self, node):
        scope = self.lexical.scopes.get(node, self.lexical.root)
        if (scope, node.id) in self.overrides:
            return self.overrides[scope, node.id]
        owner, value = scope.lookup(node.id)
        return value if owner is scope and isinstance(value, ast.AST) else None

    def text(self, node, seen=frozenset()):
        if node is None or node in seen:
            return '?'
        seen = seen | {node}
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if self.canonical(node) == 'sys.executable':
            return '<python>'
        if isinstance(node, ast.Name):
            return self.text(self.binding(node), seen)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return self.text(node.left, seen) + self.text(node.right, seen)
        if isinstance(node, ast.JoinedStr):
            return ''.join(self.text(part, seen) for part in node.values)
        if isinstance(node, ast.FormattedValue) and node.conversion == -1 and node.format_spec is None:
            return self.text(node.value, seen)
        # Only these transformations preserve a captured HEAD verbatim.
        if isinstance(node, ast.Attribute) and node.attr == 'stdout':
            return self.text(node.value, seen)
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr in {'strip', 'decode'} and not node.args and not node.keywords:
                return self.text(node.func.value, seen)
            command = self.command(node)
            if command is not None:
                argv, repo = command
                if argv == ['rev-parse', 'HEAD'] and repo is not None:
                    marker = f'\0HEAD{id(node)}\0'
                    self.heads[marker] = repo
                    return marker
        return '?'

    def sequence(self, node, seen=frozenset()):
        if node is None or node in seen:
            return None
        seen = seen | {node}
        if isinstance(node, ast.Name):
            return self.sequence(self.binding(node), seen)
        if isinstance(node, (ast.List, ast.Tuple)):
            items = []
            for item in node.elts:
                if isinstance(item, ast.Starred):
                    expanded = self.sequence(item.value, seen)
                    items.extend(expanded if expanded is not None else [item])
                else:
                    items.append(item)
            return items
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left, right = self.sequence(node.left, seen), self.sequence(node.right, seen)
            return left + right if left is not None and right is not None else None
        if isinstance(node, ast.Call) and self.canonical(node.func) == 'shlex.split' and len(node.args) == 1 and not node.keywords:
            return self.shell_words(self.text(node.args[0]))
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return self.shell_words(node.value)
        return None

    @staticmethod
    def shell_words(command):
        # shlex does not interpret expansions or shell operators. Reject them.
        if any(char in command for char in {';', '|', '&', '<', '>', '`', '$', chr(92), chr(10)}):
            return None
        try:
            return shlex.split(command)
        except ValueError:
            return None

    def repository(self, node):
        if node is None:
            return ('cwd',)
        if isinstance(node, ast.Call) and self.canonical(node.func) == 'str' and len(node.args) == 1:
            return self.repository(node.args[0])
        if isinstance(node, ast.Name):
            scope = self.lexical.scopes.get(node, self.lexical.root)
            owner, value = scope.lookup(node.id)
            values = owner.bindings.get(node.id, [])
            if owner is not scope or len(values) > 1:
                return None
            return (id(owner), node.id)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return ('path', node.value)
        return None

    def program(self, node, seen=frozenset()):
        if node is None or node in seen:
            return '?'
        seen = seen | {node}
        name = self.canonical(node)
        if name == 'sys.executable':
            return '<python>'
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value.rsplit('/', 1)[-1]
        if isinstance(node, ast.Name):
            scope = self.lexical.scopes.get(node, self.lexical.root)
            owner, value = scope.lookup(node.id)
            if isinstance(value, ast.AST):
                return self.program(value, seen)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            return self.program(node.right, seen)
        if isinstance(node, ast.Call):
            if name in {'str', 'pathlib.Path', 'Path', 'shutil.which'} and node.args:
                return self.program(node.args[0], seen)
            if isinstance(node.func, ast.Attribute) and node.func.attr in {'resolve', 'absolute'}:
                return self.program(node.func.value, seen)
        return '?'

    def forwarded(self, node):
        scope = self.lexical.scopes.get(node)
        owner = self.lexical.owners.get(scope)
        if not isinstance(owner, (ast.FunctionDef, ast.AsyncFunctionDef)) or owner.name not in self.WRAPPERS or not owner.args.vararg:
            return False
        arg = node.args[0] if node.args else None
        if not isinstance(arg, ast.List) or not arg.elts or self.text(arg.elts[0]) != 'git':
            return False
        tail = arg.elts[-1]
        prefix, _ = self.git_arguments(arg.elts[1:-1], None)
        if prefix or len(scope.bindings.get(owner.args.vararg.arg, [])) != 1:
            return False
        return (isinstance(tail, ast.Starred) and isinstance(tail.value, ast.Name)
                and tail.value.id == owner.args.vararg.arg
                and all(not isinstance(item, ast.Starred) for item in arg.elts[:-1]))

    def contexts(self, node):
        loops = []
        parent = self.parents.get(node)
        while parent is not None:
            if isinstance(parent, ast.For) and isinstance(parent.target, ast.Name):
                values = self.sequence(parent.iter)
                if values is not None:
                    scope = self.lexical.scopes[parent]
                    loops.append([((scope, parent.target.id), value) for value in values])
            parent = self.parents.get(parent)
        # Unsupported or excessive combinations keep their unresolved values.
        if not loops or any(len(values) > 32 for values in loops):
            return [{}]
        return [dict(rows) for rows in product(*loops)] if len(loops) < 3 else [{}]

    def command(self, node):
        name = self.canonical(node.func)
        leaf = name.rsplit('.', 1)[-1]
        keywords = {kw.arg: kw.value for kw in node.keywords}
        repo = self.repository(keywords.get('cwd'))
        if isinstance(node.func, ast.Name) and (name in self.WRAPPERS or leaf in self.WRAPPERS):
            args = list(node.args)
            if len(args) > 1 and self.text(args[0]) not in HISTORY_VERBS | self.SETUP_VERBS and not self.text(args[0]).startswith('-'):
                repo = self.repository(args.pop(0))
            return self.git_arguments(args, repo)
        if leaf not in self.LAUNCHERS:
            return None
        if not (name.startswith(('subprocess.', 'os.')) or '.subprocess.' in name):
            return None
        arg = node.args[0] if node.args else keywords.get('args')
        executable = keywords.get('executable')
        if leaf.startswith('exec') or leaf.startswith('posix_spawn'):
            executable = arg
            program = self.text(arg)
            if program.rsplit('/', 1)[-1] != 'git' and program != '?':
                return None
            argv = list(node.args[1:-1] if leaf.endswith('e') else node.args[1:]) if leaf.startswith('execl') else self.sequence(node.args[1]) if len(node.args) > 1 else None
        else:
            argv = self.sequence(arg)
        if argv is None:
            # A known non-Git executable needs no revision proof.
            if isinstance(arg, (ast.List, ast.Tuple)) and arg.elts:
                first = self.canonical(arg.elts[0])
                if first in {'sys.executable'}:
                    return None
            return ['?'], repo
        words = [item if isinstance(item, str) else self.text(item) for item in argv]
        if argv and isinstance(argv[0], ast.AST) and executable is None:
            program = self.program(argv[0])
            if program != '?':
                words[0] = program
        if not words:
            return None
        if executable is not None:
            program = self.text(executable).rsplit('/', 1)[-1]
            if program not in {'git', '?'}:
                return None
            words[0] = program
        if words[0].rsplit('/', 1)[-1] != 'git':
            return (['?'], repo) if '?' in words[0] else None
        return self.git_arguments(argv[1:], repo)

    def git_arguments(self, argv, repo):
        words = [item if isinstance(item, str) else self.text(item) for item in argv]
        index = 0
        while index < len(words) and words[index].startswith('-'):
            option = words[index]
            if option in {'-C', '--git-dir', '--work-tree', '-c'}:
                if index + 1 >= len(words):
                    return ['?'], repo
                if option != '-c':
                    value = argv[index + 1]
                    repo = self.repository(value) if isinstance(value, ast.AST) else ('path', value)
                index += 2
            elif option in {'--no-pager', '--no-optional-locks'}:
                index += 1
            else:
                return ['?'], repo
        return words[index:], repo

    def safe_revision(self, revision, repo):
        revision = revision.split(':', 1)[0]
        return all(part == 'HEAD' or (repo is not None and self.heads.get(part) == repo)
                   for part in re.split(r'\.\.\.?', revision))

    def inspect(self, argv, repo):
        if not argv:
            return []
        verb, *operands = argv
        if verb in {'init', 'config', 'add', 'status', 'ls-files', 'clean', 'rm', 'mv',
                    'check-ignore', 'check-attr', 'hash-object', 'write-tree',
                    'index-pack', 'count-objects', 'gc'}:
            return []
        if verb == 'commit':
            while operands:
                operand = operands.pop(0)
                if operand in {'-m', '-qm', '-am', '-qam', '-aqm', '-F', '--message', '--file'} and operands:
                    operands.pop(0)
                elif operand not in {'-q', '--quiet', '-a', '--all', '--allow-empty',
                                     '--no-gpg-sign', '--no-verify', '--amend'}:
                    return [operand]
            return []
        if verb == 'rev-parse':
            if operands and all(value in {'--show-toplevel', '--show-prefix', '--git-dir',
                                         '--is-shallow-repository', '--is-inside-work-tree'} for value in operands):
                return []
            return [] if all(value in {'HEAD', '--verify', '--short', '--abbrev-ref',
                                       '--symbolic-full-name'} for value in operands) else operands

        if verb not in HISTORY_VERBS:
            return ['unproved Git command']
        if verb == 'cat-file' and operands and operands[0] in {'blob', 'tree', 'commit', 'tag'}:
            operands.pop(0)
        revisions = []
        while operands:
            operand = operands.pop(0)
            if operand == '--':
                break
            if operand in self.VALUE_OPTIONS:
                if not operands:
                    return ['missing option value']
                operands.pop(0)
            elif operand.startswith('-'):
                option = operand.split('=', 1)[0]
                if option not in self.FLAG_OPTIONS and not ('=' in operand and option in self.VALUE_OPTIONS):
                    return [operand]
            else:
                revisions.append(operand)
                if not self.safe_revision(operand, repo):
                    return revisions
                if verb in {'archive', 'cat-file', 'ls-tree'}:
                    return []
        return []

    def sites(self):
        found = set()
        for node in ast.walk(self.tree):
            if not isinstance(node, ast.Call) or self.forwarded(node):
                continue
            for context in self.contexts(node):
                self.overrides = context
                command = self.command(node)
                if command is not None:
                    argv, repo = command
                    if bad := self.inspect(argv, repo):
                        found.add((node.lineno, argv[0] if argv else '?', tuple(bad)))
        return [(line, verb, list(bad)) for line, verb, bad in sorted(found)]


def history_read_sites(source):
    """Reject Git launches unless revision operands have a local HEAD proof.

    Unsupported argv, imports, shell syntax and ambiguous bindings fail closed.
    No source is executed or imported while scanning it.
    """
    return _HistoryGuard(source).sites()


def history_tree_reads(root):
    found = {}
    for path in sorted((root / 'tests').rglob('*.py')):
        relative = path.relative_to(root / 'tests').as_posix()
        if relative in HISTORY_TOOLS:
            continue
        source = path.read_bytes()
        sites = history_read_sites(source)
        if exception := HISTORY_PROCESS_EXCEPTIONS.get(relative):
            pins, allowed, reason = exception
            assert reason.strip()
            if raw_sha256(source) in pins:
                sites = [site for site in sites if (site[0], site[1]) not in allowed]
        if sites:
            found[str(path.relative_to(root))] = sites
    return found


def test_historical_inputs_come_from_the_committed_tree():
    assert all(reason.strip() for reason in HISTORY_TOOLS.values())
    assert not history_tree_reads(Path(__file__).resolve().parents[1])


@pytest.mark.parametrize("verb", sorted(HISTORY_VERBS))
@pytest.mark.parametrize("operand", ["'deadbeef:source'", '"main:source"',
    'f"{PIN}:{path}"', 'PIN + ":" + path', 'f"{revision}:{path}"'])
@pytest.mark.parametrize("prefix", ['"git", ', "'git', '-C', root, "])
def test_history_guard_catches_retained_revisions(verb, operand, prefix):
    source = ('PIN = "deadbeef"\ndef read(revision=PIN):\n'
              f'    return subprocess.check_output([{prefix}{verb!r}, {operand}])\n')
    assert history_read_sites(source)


@pytest.mark.parametrize("verb", sorted(HISTORY_VERBS))
def test_history_guard_allows_head(verb):
    assert not history_read_sites(f'subprocess.run(["git", "-C", root, {verb!r}, "HEAD"])')


def test_history_guard_checks_pinned_git_wrappers():
    assert history_read_sites('PIN = "deadbeef"\n_git(root, "show", PIN + ":file")')
    assert history_read_sites('_git(root, "show", f"{revision}:file")')
    assert not history_read_sites('source_head = _git(root, "rev-parse", "HEAD").stdout.strip()\n'
                                 '_git(root, "diff", f"{source_head}...HEAD")')
    assert not history_read_sites('subprocess.run(["git", "cat-file", "blob", "HEAD:file"])')
    assert history_read_sites('subprocess.run(["git", "show", "HEAD", "main"])')



@pytest.mark.parametrize("command", [
    'subprocess.run("git show deadbeef:file", shell=True)',
    'subprocess.check_output(shlex.split("git show deadbeef:file"))',
    'subprocess.Popen("git archive deadbeef", shell=True)',
    'os.system("git show deadbeef:file")',
    'os.popen("git show deadbeef:file")',
    'os.execv("git", ["git", "show", "deadbeef:file"])',
    'os.execv("git", ["disguise", "show", "deadbeef:file"])',
    'subprocess.run(["disguise", "show", "deadbeef:file"], executable="git")',
    'command=["git","show","HEAD"]\ncommand[2]="deadbeef:file"\nsubprocess.run(command)',
    'os.execlp("git", "git", "show", "deadbeef:file")',
    'subprocess.run(["git"] + ["show", "deadbeef:file"])',
    'from constants import SHOW\nsubprocess.run(["git", SHOW, "deadbeef:file"])',
    'from constants import COMMAND\nsubprocess.run(COMMAND)',
    'from constants import GIT\nsubprocess.run([GIT, "show", "deadbeef:file"])',
    'from subprocess import check_output as capture\ncapture("git show deadbeef:file", shell=True)',
    'import subprocess as sp\nsp.run(args=["git", "show", "deadbeef:file"])',
    'subprocess.run(["git", "show", "HEAD", *revisions])',
    'subprocess.run(["git", "show", revision()])',
    'subprocess.run(["git", "checkout", "deadbeef"])',
    'subprocess.run(["git", "rev-parse", "deadbeef"])',
    'subprocess.run(shlex.split("git show HEAD", posix=False))',
    'PIN="HEAD"\nsubprocess.run(["git","show",f"{PIN!r}"])',
    'launch=subprocess.run\nlaunch(["git","show","deadbeef:file"])',
    'from helpers import _git as invoke\ninvoke(root,"show","deadbeef:file")',
    'subprocess.run([prefix + "git","show","deadbeef:file"])',
    'def _git(*args):\n return subprocess.run(["git","show","deadbeef:file",*args])\n_git("HEAD")',
    'subprocess.run("git show HEAD; git show deadbeef:file", shell=True)',
    'subprocess.run("git show HEAD$(git rev-parse main)", shell=True)',
    'subprocess.run("git show \'HEAD", shell=True)',
    'subprocess.run(["git", "show", "--all"])',
    'subprocess.run(["git", "log", "--branches=main"])',
    'subprocess.run(["git", "show", "--unknown", "HEAD"])',
    'subprocess.run(["git", "cat-file", "--batch"], input=query)',
    'PIN="HEAD"\nPIN="deadbeef"\nsubprocess.run(["git", "show", PIN])',
    'PIN="deadbeef"\ndef unrelated(PIN="HEAD"):\n pass\nsubprocess.run(["git", "show", PIN + ":file"])',
    'PIN="HEAD"\ndef read():\n subprocess.run(["git", "show", PIN])',
    'def read(PIN="HEAD"):\n subprocess.run(["git", "show", PIN])',
    'class Other:\n PIN="HEAD"\nsubprocess.run(["git", "show", PIN])',
    'PIN="deadbeef"\nclass Other:\n PIN="HEAD"\nsubprocess.run(["git", "show", PIN])',
    'source_head=_git(other_root,"rev-parse","HEAD").stdout.strip()\n_git(root,"diff",f"{source_head}...HEAD")',
    'source_head=_git(root,"rev-parse","HEAD").stdout.strip()\nsource_head="deadbeef"\n_git(root,"diff",f"{source_head}...HEAD")',
    'def other():\n source_head=_git(root,"rev-parse","HEAD").stdout.strip()\n_git(root,"diff",f"{source_head}...HEAD")',
])
def test_history_guard_denies_unproved_launches(command):
    assert history_read_sites(command)


@pytest.mark.parametrize("command", [
    'subprocess.check_output(["git", "-C", temporary_repo, "show", "HEAD:file"])',
    'subprocess.run(["git", "-C", temporary_repo, "diff", "HEAD", "--", "file"])',
    'subprocess.Popen(["git", "-C", temporary_repo, "cat-file", "blob", "HEAD:file"])',
    'subprocess.run("git show HEAD:file", shell=True)',
    'subprocess.check_output(shlex.split("git show HEAD:file"))',
    'os.system("git show HEAD:file")',
    'os.popen("git show HEAD:file")',
    'os.execv("git", ["git", "show", "HEAD:file"])',
    'os.execlp("git", "git", "show", "HEAD:file")',
    'subprocess.run(["git"] + ["show", "HEAD:file"])',
    'PIN="HEAD"\nsubprocess.run(["git", "show", PIN + ":file"])',
    'PIN="deadbeef"\ndef read():\n PIN="HEAD"\n subprocess.run(["git", "show", PIN + ":file"])',
    'source_head=_git(root,"rev-parse","HEAD").stdout.strip()\n_git(root,"diff",f"{source_head}...HEAD")',
    'source_head=subprocess.check_output(["git","-C",str(root),"rev-parse","HEAD"], text=True).strip()\nsubprocess.run(["git","-C",root,"diff",f"{source_head}...HEAD"])',
    'subprocess.run([sys.executable, "-c", program])',
])
def test_history_guard_accepts_proved_launches(command):
    assert not history_read_sites(command)



def test_history_guard_exceptions_require_exact_source_bytes(tmp_path, monkeypatch):
    directory = tmp_path / 'tests'
    directory.mkdir()
    path = directory / 'example.py'
    source = b'subprocess.run(command)\n'
    path.write_bytes(source)
    monkeypatch.setitem(HISTORY_PROCESS_EXCEPTIONS, 'example.py', (
        (raw_sha256(source),), ((1, '?'),), 'Audited fixture Python command.'))
    assert not history_tree_reads(tmp_path)
    path.write_bytes(source + b'subprocess.run(["git", "show", "deadbeef:file"])\n')
    assert len(history_tree_reads(tmp_path)['tests/example.py']) == 2


def test_every_committed_history_input_has_a_verified_blob():
    pins = set()
    for revision, row in history_inputs.history_manifest()["revisions"].items():
        for path, pin in row["files"].items():
            history_inputs.source_bytes(revision, path)
            pins.add(pin["sha256"])
    assert pins == history_inputs.history_blobs().keys()


@pytest.mark.parametrize("damage", ["bytes", "sha256"])
def test_history_inputs_refuse_damaged_bytes(monkeypatch, damage):
    revision, row = next(iter(history_inputs.history_manifest()["revisions"].items()))
    path, pin = next(iter(row["files"].items()))
    raw = history_inputs.source_bytes(revision, path)
    damaged = raw + b"!" if damage == "bytes" else bytes([raw[0] ^ 1]) + raw[1:]
    monkeypatch.setattr(history_inputs, "history_blobs", lambda: {pin["sha256"]: damaged})
    with pytest.raises(AssertionError, match="byte count differs" if damage == "bytes" else "digest differs"):
        history_inputs.source_bytes(revision, path)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    reads = history_tree_reads(parser.parse_args().root.resolve())
    for path, sites in reads.items():
        print(path, sites)
    raise SystemExit(bool(reads))
