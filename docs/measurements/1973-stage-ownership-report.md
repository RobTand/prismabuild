# #1973 fixture verification

No live stage inventory or reclaim was performed. The incident's 271.4-GiB
figure was not remeasured. See [report semantics](../stage_ownership_report.md).

Initial implementation check, using the provided admitted x86/Python 3.14 environment:

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

## Review repair: destination containment and explicit exclusions

Rechecked both O5 failures against the implementation above, using new
supplemental fixture tests (not edits to the acceptance file):

```sh
python -m pytest tests/test_stage_ownership_bounds.py -q -k 'outside_mover or fifo' --basetemp="$TMPDIR/pytest"
```

Before the repair, both cases failed: an outside mover's manifest falsely owned
11 bytes inside the selected stage, and a top-level FIFO was absent from
`excluded`.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"601e5c722c845f259da2544e0dfc68b91b4fc2d361db28848275d5776edd946e","size":3611}},"operation":"3fc453ecea1ddc30ff7f2e79a17481fa047630aed4d6d1a310c4bd6387b43a95","receipt":"660c5cd47847d1483ae8e28113a6c8fea5db727131c6e53a38210d632f598948"}]

Report-mode manifest paths now use the declared destination instead of the
selected stage root. Outside paths retain provenance but are excluded from
inside ownership joins. Strict reclaim's conservative selected-stage protection
set is unchanged. Top-level non-regular entries, including FIFOs, are explicitly
excluded. Added regressions also cover a nested destination and missing/invalid
destinations remaining unknown rather than importing source-stage hints.

Final repair regression check used the same four-file command shown above:
**147 passed, 1 teardown error; exit 1**. This includes all 14 acceptance test
calls, 14 supplemental calls and the existing reclaim/restore, publication
closure and reader-lifetime cases. It is not a clean acceptance pass.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"86885af549dd59c0f0bc5dda51b8f5cb3b7a24c0df10d5002e87b8f8f1679e91","size":2537}},"operation":"3b9985398d492d069da1403252652988fa5f3e4531fe6f3820fb1aeb264f0887","receipt":"cbfafc43e78d51ca7bae001fe3ad173facc7312f63886f2b1f48213074ebdd76"}]

### O6 remains blocked by the acceptance fixture (#2013)

Re-ran the exact required check after the repair:

```sh
python -m pytest tests/test_stage_ownership_report.py -q -k 'budget or unreadable_directory' --basetemp="$TMPDIR/pytest"
```

Observed **4 passed, 10 deselected, 1 teardown error; exit 1**. The same patched
`os.scandir` wrapper rejects integer descriptor 11 at line 397. Source inspection
identifies the cleanup interaction: the autouse `d38_config` fixture requests
`monkeypatch` before `tmp_path`; reverse teardown therefore cleans `tmp_path`
before undoing the patch. With `tmp_path_retention_policy = "failed"`, a passing
body triggers descriptor-based cleanup through `shutil.rmtree`.

The minimum external repair is for the acceptance-file owner to fix the wrapper
under the existing #2013 issue. This work is forbidden to edit that acceptance
file. No application workaround, test suppression, retention-policy change or
duplicate issue was added. O6's required check remains failing, not met.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"80b1d3721b0d3327f667754f813eeca27f6d39528e650da5528cfc5830f95f8b","size":1730}},"operation":"4085ede3f957bcebdfa90e28fa1ec9d03e8e69f7f633a03bb511c2413683c8cb","receipt":"13b97cfab10d620277d3aa609b632a1be3bf9729c9f0102a76c8393ccf298877"}]

### Second review: full traceback establishes the O6 fixture failure

The exact O6 command above was rechecked on the unchanged implementation:
**4 passed, 10 deselected, 1 teardown error; exit 1**.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"c73ec82b3e9fe4f0a72323b077c559900c08ab8042bda371843ba4ead744ef73","size":1730}},"operation":"b351cf37947c5c5bdd3992106547a1c928d6f8a32c6a32b8cf07dadcd35a99fd","receipt":"1f3c2fce37179c3edd8317058fb0272981aa2133c6ca6070e9c37eecbcc3de95"}]

One diagnostic execution retained the full traceback and fixture order:

```sh
python -m pytest tests/test_stage_ownership_report.py::test_unreadable_directory_keeps_useful_report -q --full-trace --setup-show --basetemp="$TMPDIR/pytest"
```

Observed **1 passed, 1 teardown error; exit 1**. The retained stdout now directly
establishes the cleanup caller, rather than merely inferring it from source:

- `monkeypatch` is set up before `tmp_path` and torn down after it.
- After the passing call, `_pytest/tmpdir.py:310` calls
  `rmtree(path, ignore_errors=True)` under the configured `failed` retention
  policy.
- `shutil.py:777` calls `os.scandir(topfd)` during descriptor-safe deletion.
- The still-installed acceptance wrapper reaches `os.fspath(11)` at
  `tests/test_stage_ownership_report.py:397` and raises `TypeError`.

This occurs after the report has returned and its unreadable-directory
assertions have passed. The report's own scanner calls `os.scandir` with paths
and records the injected `PermissionError` as incomplete evidence. Application
exception handling cannot catch a later pytest finalizer failure. The full
trace confirms that the missing integer-descriptor support is in the acceptance
wrapper, not in the report.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"8dc875ab05f3a2fd82d5f886c73087c2655a046e8d531fea54157af6d856d546","size":38040}},"operation":"4fd256041a37825e4581436faa7df10bdcb39c942a122d1a3f118a99bd3ac1ad","receipt":"f364db03f17dc560ddcfe2bcf22e51b93dbab3dcae20baecc83d0b3e556b88bd"}]

The four-file acceptance/supplemental/reclaim/reader command above was also
rechecked: **147 passed, 1 teardown error; exit 1**, with the same failure.
The acceptance source digest remains
`ed9f85edebc45df901f5bdacfa9b4d63d603335bffa477af12b91f9a8542b505`.
No production code, acceptance file, shared fixture, discovery, cleanup policy
or assertion was changed. No suppression or global monkeypatch restoration
was introduced. O6's required passing check cannot be supplied by an in-scope
application repair under these constraints; acceptance-owner correction under
#2013 remains necessary. This is a diagnosed check dependency, not a passing O6
result or completed acceptance. No full-suite or live-stage check was performed.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"397c1cf849410c1fec01a6f4b3b63c5eb170057b0cbe92de848269e758b936f0","size":2536}},"operation":"310058f583bc89b2a035e9c7fc19847fa708b2e48ef76d31a4f7a061293dbac2","receipt":"d42dabdb5058aad63d305d56e71aaee1e7d70ca1ceae345222d2788d4026fead"}]
