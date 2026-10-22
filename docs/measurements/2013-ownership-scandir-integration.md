# Ownership acceptance scandir integration (#2013 / #2017)

## Adopted source and scope

Base `c0521b8ed6abeb38b30188e85e5ad7cb670c7be8` lacks the #1973 API and acceptance module. This integration adopts the existing implementation from `70dc050a08351da817b00b06213eafc88d6ad5d1` and its destination-binding repair `1226f9ffc211321157270b69c12d3a0f26ee46ff`, not a newly implemented scanner. The optional bounded census hooks, evidence-only live-claim join, sibling report collector, and runtime publication member are necessary dependencies of that existing implementation. Existing report documentation and bounds tests are adopted with them. Historical measurement claims are not imported.

At initial integration, the adopted production files, report guide, and bounds tests were byte-compared against `1226f9ffc211321157270b69c12d3a0f26ee46ff` and matched exactly. Subsequent review found three defects in that retained report implementation; the targeted corrections and new regression evidence are recorded below. The design addition is the existing six-line report-mode documentation patch. Historical Git is only an integration source; the resulting tests use tree-contained inputs, not historical Git objects.

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

## Review repair: incomplete metadata must remain uncertain

The three review findings were rechecked against the integrated code before repair. Added regressions in `tests/test_stage_ownership_bounds.py` reproduced the missing-directory false success, two malformed-manifest IndexErrors, and the malformed-leads TypeError:

`python -m pytest tests/test_stage_ownership_bounds.py -q -k 'missing_configured or malformed' --basetemp="$TMPDIR/pytest"`

Observed before repair: **4 failed, 14 deselected**, exit 1. Retained stdout was independently read. This is fresh reproduction of the findings, not a claim to have replayed the review operation.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"82b7512f515940ad96c976c4a15313347fb375956aa877666a4b6f66d934b656","size":12767}},"operation":"84ca48eb727267b9cf8c85a1d5707960e7ba270b6a9b9b51b018eab888e50914","receipt":"040f72e9352eaf96b1b98785ed7f6be788d6b15dc849dec7997937dd87cb4be2"}]

Corrections are confined to the report integration:

- `_IO.scandir` propagates missing-directory errors. Required fragment/residency and live-state namespaces now produce incomplete attribution rather than successful empty censuses. Movement/plan and holder callers explicitly allow initial absence of their optional, lazily created namespaces. Disappearance after opening remains an error.
- The bounded manifest reader verifies structure with `data_manifest.validate_data_manifest`, the same validator used by the strict reader, before expanding read-plan indices. Invalid manifests retain observations and produce unresolved-manifest diagnostics.
- The evidence-only live-claim join validates `residency.leads` as a list of nonempty strings. Malformed collections/entries are reported through an optional diagnostic sink. Independently known live owners and other valid mover contributions survive; the default lifecycle path is unchanged.

Additional regressions cover valid/absent lead arrays, malformed scalar/string/dict/entry values, optional namespace absence, and failure during iteration of an optional namespace. The original acceptance module, descriptor guard, protected contract test and immutable source artifact remain unchanged by this repair.

### Final integrated verification

`python -m pytest tests/test_stage_ownership_scandir_contract.py tests/test_stage_ownership_report.py tests/test_stage_ownership_bounds.py tests/test_a_source_mark_copy_with_a_proven_original_is_reclaimed.py tests/test_reader_lease_lifetime.py tests/test_planner_filed_rows_are_staged.py tests/test_798_nonregular_census_review.py tests/test_a_concluded_consumers_plan_leaves_the_census.py tests/test_a_range_decision_takes_its_censuses_once.py tests/test_an_egress_census_before_the_lock_decides_nothing.py -q --basetemp="$TMPDIR/pytest"`

Observed: **199 passed**, exit 0, including all new regressions, the original report acceptance coverage, published-generation execution, reader lifetime, planner and shared-census regressions. Retained stdout was independently read.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"c18ae25dcf3e7c0b0f3a38bc68d1111b5eb5d27693e57b239a142edcb0e1c060","size":3351}},"operation":"d46abf3bd4e5f715f970e2b8f0b0770244ade959c694535c0af7fbaaa8f12d8f","receipt":"25c17cf893c87444fec677424eab49bf39196a323760be26d08f8056d9efc1e8"}]

`python -m pytest tests/test_stage_ownership_scandir_contract.py -q --basetemp="$TMPDIR/pytest"`

Observed: **1 passed**, exit 0, for the exact O1 command after the review repairs.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"0fb90eb90facb2e6ed61e9c1514c4173340a4b6b41a52d3524aa11324e55edce","size":1164}},"operation":"2fe71ef8145565da7c91a38e2bfc634080f05ad0ba748fd7172124fd80606da9","receipt":"ea50091f08bd4cca8990def3c22cb9cc27437352bb1400f10e54121287c1267b"}]

The repair's final diff was inspected and `git diff --check` passed. Full-suite and live deployment/stage checks remain unperformed.
