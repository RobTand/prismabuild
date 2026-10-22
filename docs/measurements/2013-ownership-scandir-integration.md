# Ownership acceptance scandir integration (#2013 / #2017)

## Adopted source and scope

Base `c0521b8ed6abeb38b30188e85e5ad7cb670c7be8` lacks the #1973 API and acceptance module. This integration adopts the existing implementation from `70dc050a08351da817b00b06213eafc88d6ad5d1` and its destination-binding repair `1226f9ffc211321157270b69c12d3a0f26ee46ff`, not a newly implemented scanner. The optional bounded census hooks, evidence-only live-claim join, sibling report collector, and runtime publication member are necessary dependencies of that existing implementation. Existing report documentation and bounds tests are adopted with them. Historical measurement claims are not imported.

The adopted production files, report guide, and bounds tests were byte-compared against `1226f9ffc211321157270b69c12d3a0f26ee46ff`: they match exactly. The design addition is the existing six-line report-mode documentation patch. Historical Git is only an integration source; the resulting tests use tree-contained inputs, not historical Git objects.

The complete original 463-line acceptance module is restored from `tests/fixtures/stage_ownership_unreadable_directory.py.txt` (SHA-256 `ed9f85edebc45df901f5bdacfa9b4d63d603335bffa477af12b91f9a8542b505`, originally blob `ad54ad794a4eb737652e8d8c557fc48f9080ff0f` at `e73cb5388fcd34ceb36fb1943274b48c15a84c5c`). A byte comparison verifies that its only difference from that artifact is:

```diff
-        if os.fspath(path) == str(stage / "blocked"):
+        if isinstance(path, (str, os.PathLike)) and os.fspath(path) == str(stage / "blocked"):
```

The saved native `return real_scandir(path)` and intended PermissionError remain unchanged. In particular, the original incomplete-report, incomplete-blocked-row, seven-readable-byte, and blocked-directory-error assertions are intact. This is one shared #2013/#2017 repair, not a second implementation. Neither protected acceptance input was edited. No production workaround, assertion weakening, skip, cleanup suppression, fixture-order change, or pytest retention change was made.

## Verification

All tests below ran through the supplied admitted x86/Python 3.14 runner, with temporary files outside the worktree. The first run reproduces the actual restored nested wrapper through AST extraction; it is not a historical composed-suite replay.

### Before the guard

`python -m pytest tests/test_stage_ownership_scandir_contract.py -q --basetemp="$TMPDIR/pytest"`

Observed: **1 failed**, call phase, `os.fspath(11)` raises TypeError at the restored acceptance module's line 397. Teardown completed.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"07a586717dffc7077bf8237ebb015a39dbbafa634d489035980188b092f8ef93","size":3431}},"operation":"07d101f0fc32d925f0485f664873852e323ff2a1a5a4930d6f3fb45f90ca7d59","receipt":"9d68cc9da1ab8e2af0dfba9bce5af07c9a9d462674bb34654809e0b9ed883e1e"}]

### Integrated acceptance and regressions after the guard

`python -m pytest tests/test_stage_ownership_scandir_contract.py tests/test_stage_ownership_report.py tests/test_stage_ownership_bounds.py tests/test_a_source_mark_copy_with_a_proven_original_is_reclaimed.py tests/test_reader_lease_lifetime.py -q --basetemp="$TMPDIR/pytest"`

Observed: **148 passed**, exit 0, no teardown error. Retained stdout was independently read. This includes actual report integration and the existing published-generation execution test, not merely wrapper replay.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"c505e6990ae1b1e34c075f570fd86ec0556a44e4aa13ac547558f066b3c7c442","size":2172}},"operation":"7ff4fc6dda6168c5dd0657a1b6b65ae0d6b4669ca2c9c24e6cf655afc9fc3798","receipt":"22dee8ddd1079a2054a70515cb61caf2b7285c22e43a1eeb4fa90af1bd9e48dd"}]

### Exact O1 command after the guard

`python -m pytest tests/test_stage_ownership_scandir_contract.py -q --basetemp="$TMPDIR/pytest"`

Observed: **1 passed**, exit 0. Retained stdout was independently read. Covers denied string/Path inputs, ordinary forwarding, readable and blocked directory descriptors, descriptor lifetime, and recursive cleanup with the global replacement installed.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"bb9768af2f3f0fb575a9b8105f5cd83d90634158321866eee432be2fe14942b8","size":1164}},"operation":"82efc3d303cb8b6ea8fc398ff34d2ca8f621d09e67207fd099e66c52da440c36","receipt":"dbfabba137befc12bc97d574580d9128b6ef7609e27eefce33ec0c8cff490ca6"}]

### Additional shared-census regressions

`python -m pytest tests/test_798_nonregular_census_review.py tests/test_a_concluded_consumers_plan_leaves_the_census.py tests/test_a_range_decision_takes_its_censuses_once.py tests/test_an_egress_census_before_the_lock_decides_nothing.py -q --basetemp="$TMPDIR/pytest"`

Observed: **23 passed**, exit 0.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"1c373f315b1ac28d5c7e796a3ad5a73f0ef13abd875fd55cb570a630ef956db1","size":1915}},"operation":"8f561ce5ec5eeff074fbb4150226a7c6bbf8704d2d636220c687a5972bc0e6f0","receipt":"11259b334af4d1ad2096aa00b3570e4f9f9a86e64cf2f5ba6d6bed8369b6610c"}]

`git diff --check` completed successfully. Full repository-suite execution and live deployment/stage measurement were not performed; these scoped results do not establish either.
