# #1939: field inventory merge repair

## Scope

Resolved the content conflict in `tests/compat/field_sets.json` between bell
repair HEAD `695a5ad3a5b946929471ac5bd076f92bea43b257` and incoming main
`1e6651199fa74fa501e6b1fa08f10a5c18dd75d5`.

The merged inventory has 571 rows: all 569 incoming-main rows plus the two
reviewed bell witnesses (`Topic.required` and `InotifyQueue.consume`'s
`affected` coverage set). This retains main's `cpu_guest` claim-scoped field
and `retry_scope` optional publication key. Rows retain the inventory tool's
canonical ordering. No runtime or test source was changed by this repair;
the staged incoming-main changes and the self-contained v1 fixture are preserved.

## Verification

Checks ran through `run_tests` in the supplied admitted x86/Python 3.14
environment, with two CPUs, bounded native threads and temporary pytest data
outside the worktree. Retained passing stdout was read back.

Before resolution:

```sh
python -m pytest tests/test_wire_inventory.py::test_closed_field_sets_remain_pinned -q --basetemp="$TMPDIR/pytest"
```

Exit 1: one failure, `JSONDecodeError` at the unresolved conflict marker.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"3beaa79aafb2b6e0f87756fab089ecbb079d19f38e69df41830bdd3ba554987b","size":3212}},"operation":"e991109f801a45b88d2835ec97224446cabb57aa9d6ce4d2084bab95ffa4fc6d","receipt":"b3d1bafd22998db153679c4725c8c08a2a5df2aeea3278c878807b7e2638fdd3"}]

After resolution:

```sh
python -m pytest tests/test_queue_bell.py tests/test_queue_bell_v2.py tests/test_queue_bell_installed_coverage.py tests/test_queue_bell_lazy_coverage.py tests/test_queue_bell_startup_coverage.py tests/test_queue_bell_v1_compat.py tests/test_action_wait_bell_deadlines.py tests/test_wire_inventory.py tests/test_duplication_baseline.py -q --basetemp="$TMPDIR/pytest"
```

Exit 0: **209 passed**, zero failures/errors/skips, in 94.94 seconds. This
includes all 28 raw-wire/mixed-version v1 compatibility cases and verifies the
merged inventory against the merged source tree. The two warnings are the
existing multithreaded-fork deprecations in `_bounded_reader.py`.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"b39a99d8e30e81ea724f940324e818e82f766016d459f79cc535aeaec2111293","size":3353}},"operation":"f1dcf0e265f76f4323746073fb6a8e834f97dd76702e208a5f43f623a807faa4","receipt":"56d2c363d337790f33c69d056f8324d98ea561c202ea847addd0bc09086418de"}]

`git diff --check` and `git diff --cached --check` passed after content
resolution. Structured final-diff inspection found exactly two added bell
witnesses relative to incoming main, with no main rows removed.

Full repository gate, ARM, remote-NFS integration and consumer latency/outage
acceptance were not run. No publication or consumer activation was performed.

The worktree content is resolved. `git add -- tests/compat/field_sets.json`
could not mark the index resolved because creation of `index.lock` in the
external worktree Git metadata directory was denied. No permission changes or
retry were attempted; staging and committing remain with TrustLadder.
