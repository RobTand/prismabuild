# Class-image end-to-end implementation evidence

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
