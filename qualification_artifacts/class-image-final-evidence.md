# Class-image end-to-end implementation evidence

## Repair of review findings (current evidence)

Starting from reviewed HEAD `4a9fbdf5bb98b52afcdbbf804a347f56a6357010`:

- Work-start qualification no longer depends on optional owner/marker presence.
  Both direct and sealed-JSON native-child launches without either/both metadata
  fields refuse with `class_context_missing`, before client workload or CAS success.
- Shim forwarding is one operation, not a transitive descendant exemption. Real
  client helpers and same-PID exec replacements must re-enter qualification.
- Linux exec's former TID is reconciled when a nonleader thread becomes leader;
  a native executable that completes successfully no longer fails with ECHILD.
- Profilers are outside mediation, with their actual relay/payload subtree inside
  it. Py-spy uses nonblocking subprocess sampling, preserving real workload frames
  without competing for ptrace ownership. A deterministic `/proc` regression
  verifies that the profiler is untraced while its actual payload is traced.
- Two private profiler fixtures now pass the relay-status member explicitly,
  rather than assuming an argv offset inside a wrapped command. Bounded condition
  polling replaces sleeps in those modified fixture snippets. No tests were removed.
- No acceptance files, `test_pbstatus_pool.py`, goldens, field sets, public SDK
  signatures, policy values or deployment state were changed in this repair.

The new regression file plus the existing real sampler selector reproduced all
four reported production defects before repair on x86: 32 failed. New fixture
setup mistakes discovered during development were corrected before final checks.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"c6596b8f863fc32fe74c7cb5029ea2514159bfe5e85e42e40b28047467b2945f","size":34145}},"operation":"5cc04f5b231d824b1a8da5ea72fce3a176da33fa57b50998919f18733d926ef0","receipt":"6ac08bb65ba772f6ee469192d065b8ca478edbfe5d0e1419b1d6ff5b11e594f4"}]

### Acceptance, required regressions, all four guards and SDK surface

Each arm ran the following serial command on the repaired source:

```sh
python -m pytest tests/test_class_image_fleet_generation.py tests/test_class_image_config_failure.py tests/test_container_class_launch_acceptance.py tests/test_container_absolute_client_execution_acceptance.py tests/test_container_exec_regressions.py tests/test_container_class_requirements.py tests/test_container_class_admission.py tests/test_publish_runtime.py tests/test_generation_roster_aliases.py tests/test_roster_publication_boundary.py tests/test_roster_presence_status.py tests/test_container_image_inventory.py tests/test_docker_shim_global_options.py tests/test_docker_shim_cpu_affinity.py tests/test_docker_shim_memory.py tests/test_pbstatus_pool.py tests/test_a_declared_container_image_is_sealed_and_checked.py tests/test_pbrun_declares_container_images.py tests/test_docker_shim_relays_termination_1599.py tests/test_the_holder_settles_its_container_transaction_before_release.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py tests/test_client_sdk_surface.py -q --tb=short --basetemp="$TMPDIR/pytest"
```

x86: **907 passed**, exit 0.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"c0f7d238270e742acd42e78313f44636382e9aabdade26538a76be176e3400b9","size":7024}},"operation":"5bc7b97d1a3a1630c8e558f307e41439c78b2484914a97f93d31d8971c64ab3c","receipt":"0d77bbf8624d6fe0a32f6d87c24d99a7d8af637fc8f935fa21022be6b9bf665e"}]

ARM: **907 passed**, exit 0.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"9e3635d10d24040f8b6a1f3c1a8e81577dd6259b2effff6699235b343ec85446","size":7038}},"operation":"36e5f675932335cd26c2103513c4ceaa9042a83bef2ea2a290abd4d11087bb72","receipt":"8813355b9b90e111b15152b7dea6c48c90872b51cd65da75cd4d24a41ec086d2"}]

### Expanded native, Core/pool and profiler checks

```sh
python -m pytest tests/test_container_exec_regressions.py tests/test_core.py tests/test_pool.py tests/test_fleet_submit_layouts.py tests/test_client_sdk_fail_closed_edges.py tests/test_nsys_docker_refusal.py tests/test_sample_docker_coverage.py tests/test_sample_profile.py tests/test_contained_deadline_profile.py tests/test_profile_window_deadline.py tests/test_profile_summary_timeout.py -q --tb=short --basetemp="$TMPDIR/pytest"
```

**385 passed on each arm**, exit 0, including the unchanged real native sampler
workload-frame test. The two warnings are existing multithreaded-fork deprecations.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"8676a587e8e5cfcf321ea90c0dba56d4bd025699624f91396374514859a2391f","size":3878}},"operation":"ec030e82a8e3f1c8192c573300d564ac47e7e1db79832dd1f9af913f66aabf0e","receipt":"624e91bc95e05c7a9602acd0cb7f48e847052792448d8bf14d5479095b249289"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"4fea81edf0c648dafc329f932d2949170cee7687c8f1512c19d64a6d476c9cf7","size":3887}},"operation":"32f268f7e078e87518e594981eccf165c855fcc86e9f1198df988b54aa5aef64","receipt":"41fbe4ec1f169ae74b3ab98f3e3571f347dca093edce1444c8065992f149b38d"}]

The new deterministic regressions also pass under two parallel workers on both
arms: `python -m pytest tests/test_container_exec_regressions.py -q -n 2 --tb=short
--basetemp="$TMPDIR/pytest"`, **32 passed per arm**. This final rerun includes a
test-only isolation correction to clear the actual private context environment
name, rather than a nonexistent name, in the helper test. No production source
changed after the larger passing groups above.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"f7f3a80b4a4a7aa62775f1a4b281afe6dfc3cfc2c0317a109c16a547ee33cfab","size":1041}},"operation":"984103a8ca4ac8d08782e100ef876c7979d80bcf4037c21774eedf2591e0236a","receipt":"94e6b1dab048d9a50a471c6ee63d6f1e1794248aa8ef8f33f8b479e5bcaced66"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"02ab30ab19076fdddc2272d65770c2e8064d640b59a660bc5ad04382aee7d8ca","size":1055}},"operation":"de8c214a2eddd8fceffee5d41d82f63c45ffc7d100d4ab30d12aec9772c6d6bf","receipt":"72e5d7646f55e3d7c909eabd5019ddf3f4799f16db8f6bb08193afe9ecea84cf"}]

One intermediate x86 Core run also observed an unrelated failure of unchanged
`test_stable_file_identity_refuses_perpetual_one_link_ctime_churn`: DID NOT RAISE
CASTamperError. That test sleeps 1ms before real link/unlink to provoke ctime
changes. It passed in the final expanded group, but this does not establish that
its intermittent failure is fixed. No repair or extra diagnostic repetition was
performed; the observation is returned separately for ownership.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"1cb90b30f5064500c18367d5270cc7e1b375b151950840a1c7edbb75cd1ff2a0","size":6825}},"operation":"70880d1245b9812464bea408570885b7d684723509acc3baf9506544ff18a5d4","receipt":"0b690bc2967d1b6bb7bbb8bad3ba3c1fe4d9a14e4e0896f9fba7291be202e973"}]

### Compatibility remains unqualified

Both arms reran `python -m pytest tests/test_record_compatibility_*.py -q -x
--tb=short --basetemp="$TMPDIR/pytest"`. Each stopped at the first archival
writer, before record comparison: x86's selected archival TMPDIR differs from
admitted scratch; ARM cannot execute the pinned x86 Python 3.14 path. These are
the supplied baseline #2162/#2178 failures, not newly qualified comparisons.
No golden refresh, compatibility-infrastructure repair, capture or adoption was
attempted. Capture/strict adoption remain with the PB lead.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"26b26454148ec546f78b87e4ef24b1793a6b9af7ae2894d0c24017bceadfa7ac","size":120989}},"operation":"fdf3dfcadb7cfe8c72ae200a4f2d2c40df203c569257c5c751505ec28a8f9307","receipt":"f41a1fc504da00a26aac1af7489ed42407b3a3d8b8708a625e345c0a9eaf215b"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"f9a84307e796b00c8252c0b58de653bb80d40606dc7e6ef23287469c80692857","size":121564}},"operation":"dc098b1a03807c95ad5dccafd340098ff0b7172ae1058eba4c81a4a39f8a4d98","receipt":"c8aa31eb2f635b27a3e63c03a8eef683375ab2feb0853ce47292478ead59d637"}]

## Original implementation evidence (historical; superseded by repair above)

## Result and limits

The four unchanged acceptance files plus all three requested regression/guard
groups pass together: **875 passed on x86/Python 3.14 and 875 passed on
AArch64/Python 3.12**. This includes the native/inventory controls, all four
repo-wide guards, SDK surface, status, declared-image checks, publication,
roster, resource and lifecycle/termination regressions.

The checked-in declaration is explicitly pending (`classes: {}`), not approved
GB10 policy. See `docs/class_image_policy.md` for the inspected PrismaQuant
revision, projection incompatibility, unresolved approvals, Stage A prerequisite,
next-generation sequencing, endpoint and kernel boundary. No live activation,
Docker/GPU qualification or deployment was performed. Fake clients establish
control flow only. Hosts lacking kernel/procfs/memfd prerequisites refuse rather
than executing an unmediated action; host-wide qualification remains W3's work.

Existing standalone shim fixtures were migrated to executor-owned configuration
and fresh fake inventory, not a fabricated satisfied flag. All original test
functions, parametrization and assertions remain. No changes were made to the
four acceptance files or `tests/test_pbstatus_pool.py`. Initial integration
failures in roster validation and unconfigured legacy positive fixtures were
reproduced and repaired; earlier evidence documents retain those intermediate
results rather than representing them as final passes. Review also identified
and fixed forged nearer-ancestor authority and argv-only script recognition.

Private runtime dependencies now participate in runtime attestation. Closed
field-set deltas were adopted only with the prescribed maintenance tool; see
`runtime-closure-field-review.json` and `runtime-closure-field-refresh.json`.
No record goldens were changed. Compatibility is **not qualified**: bounded
checks on both arms fail before record comparison in existing archival-runtime
qualification. x86 selected TMPDIR `/home/rob/pbt17` differs from admitted scratch;
ARM cannot execute the pinned x86 Python 3.14 path. These match the supplied
baseline and existing #2162/#2178 ownership. Capture/strict adoption, if required,
remain with the PB lead; no retry or infrastructure repair was attempted.

## Final combined command (each arm)

```sh
python -m pytest tests/test_class_image_fleet_generation.py tests/test_class_image_config_failure.py tests/test_container_class_launch_acceptance.py tests/test_container_absolute_client_execution_acceptance.py tests/test_container_class_requirements.py tests/test_container_class_admission.py tests/test_publish_runtime.py tests/test_generation_roster_aliases.py tests/test_roster_publication_boundary.py tests/test_roster_presence_status.py tests/test_container_image_inventory.py tests/test_docker_shim_global_options.py tests/test_docker_shim_cpu_affinity.py tests/test_docker_shim_memory.py tests/test_pbstatus_pool.py tests/test_a_declared_container_image_is_sealed_and_checked.py tests/test_pbrun_declares_container_images.py tests/test_docker_shim_relays_termination_1599.py tests/test_the_holder_settles_its_container_transaction_before_release.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py tests/test_client_sdk_surface.py -q --basetemp="$TMPDIR/pytest"
```

x86: 875 passed in 333.07s.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"956fe1698af00b9b86699bf950e195ada8000091d5af796d0efec9231c1e1347","size":6822}},"operation":"76fbc1924b258e74e34a173cd3c5d3a989d7360d412fc5e652315c033dc199ee","receipt":"1870e3ec6875f95fbd22521478ff323973e9e1c9e5740887b17c3cb782f2c195"}]

ARM: 875 passed in 212.80s.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"dab03bc6c60ed2dadc896ebafd8776e0db23e1512b8b9241fd6e59e92d005650","size":6836}},"operation":"88be7e34d0bcbde259f85d6f0ca3ac8ed7912bb1c566d139cd839e325fe90497","receipt":"a30e72dca0a91344e9486b92432670f6df2ef2626b7eebdf7c2e53ce861ece5a"}]

## Final documentation guard rerun

After the final same-line-count design wording update:

```sh
python -m pytest tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py tests/test_client_sdk_surface.py -q --basetemp="$TMPDIR/pytest"
```

280 passed on each arm.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"43f54f4da8409e9ba8af3f2a4a95b92122fb0b6ce84e296d7f68cbd6a2f40bea","size":2124}},"operation":"7f145630d913092a51921cf1b7c1b89a87d458d4bb93c604ac0aefcaae8c7a99","receipt":"6043bfa0ca20ae21604db524c3b78be98d61069f6c7366bddec1ffc05b8fd486"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"4045feba9a6e282c813e0f82384c01d431ceb7a4077be1ba6f00df4ab73c9ad7","size":2123}},"operation":"0b771d9ccd196e016ebe522c23ca73c6847bf6468d15a63110966e6cbc6c67f0","receipt":"992030f4f762dcfe2a32e267cfedacecc8c76a711a7e1c89fe262bb2556ea957"}]

## Bounded compatibility command (each arm)

```sh
python -m pytest tests/test_record_compatibility_*.py -q -x --tb=short --basetemp="$TMPDIR/pytest"
```

Both: 1 failed, stopped at first archival writer qualification, before comparison.
Read verified stdout beginnings for the exact selected/observed runtime mismatch.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"91d440dc990ac4e12aaed21770c2d77ebca4bf8d6071a59aca34d48080102d69","size":120913}},"operation":"f10999f481cf3912bb69ce680ae7848b951902a14114f424aa7a10ec66b143ab","receipt":"ed21c68d763d5ccce2b7e264aea72efdb97cfeb569cb15060e9cdccc1d3c7838"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"550552459ef67b32844c52655ac4ea33e4e52125f61031c4be2b707e263eaecc","size":121435}},"operation":"d89e3816ce3690571b312ef90bf3309905a193ce6ab90ff2c09849c3c3843f92","receipt":"b7d5eb711054fccbba769dfcb68c21081c935c657e0b138906b3c78477d82187"}]
