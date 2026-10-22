# #1973 fixture verification

No live stage inventory or reclaim was performed. The incident's 271.4-GiB
figure was not remeasured. See [report semantics](../stage_ownership_report.md).

Final scoped check, using the provided admitted x86/Python 3.14 environment:

```sh
python -m pytest tests/test_stage_ownership_report.py tests/test_stage_ownership_bounds.py tests/test_a_source_mark_copy_with_a_proven_original_is_reclaimed.py tests/test_reader_lease_lifetime.py -q --basetemp="$TMPDIR/pytest"
```

Observed **142 passed, 1 teardown error; exit 1**. All 14 acceptance test calls
passed, as did the nine supplemental cases and the reclaim/restore, publication
closure and reader-lifetime regressions. This is not a clean acceptance pass:
`test_unreadable_directory_keeps_useful_report` errors during teardown because
its patched global `os.scandir` calls `os.fspath(11)`. The wrapper accepts paths
but not directory descriptors. Descriptor-based cleanup is a plausible trigger;
the retained traceback does not identify the full cleanup caller. Acceptance
files were not modified and no production workaround or suppression was added.

The supplemental cases cover root-level fragment ownership/provenance,
non-finite budgets, preserving readable census evidence on interruption,
symlinked ancestors, unresolved holder scopes and the actual script entrypoint.
The published-generation regression exposed the helper's initially missing
publication membership; adding it to `FLEET_SCRIPTS` fixed that failure.
Earlier development checks also caught implementation/schema errors, which were
fixed before this final check. An initially mistyped regression filename
(`tests/test_stage_reclaim.py`) produced a collection error; it was replaced by
the existing descriptive regression filename above.

Full-suite and live-system qualification were not run. Final source diff was
inspected and `git diff --check` passed.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"0e5ea7bfca0b403c62bffc73c3655982efd6d9a49a67e12fd1f3eeb093127aed","size":2455}},"operation":"48bbc0cd96a3a10745dfe5ea3df427421e4ca46190a90625f56285ffc8e7e2d7","receipt":"fa7116442ee86be8cd53bc0c78fa7e63fa432cb8febb494c8c2730f1bb7d7ecd"}]
