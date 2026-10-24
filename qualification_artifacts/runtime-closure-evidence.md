# Container runtime closure integration evidence

## Scope and findings

This work integrates the newly supplied container execution helpers into worker runtime source attestation and checks packaging. It does not publish or activate a runtime, edit acceptance tests, or change fleet deployment state.

The initial guard run reproduced three package-owned failures: `test_worker_core_has_no_unattested_repository_imports` reported `container_images.py` and `_container_exec.py`; wire inventory noticed the new proc-stat local named `fields`; duplication inventory found `_run` in `_container_exec.py`. Initial result: **479 passed, 3 failed** (operation `4dec407618d726f4d20e221dbfcabfd1d6bab0cdc195109d25851bc396f5d712`).

Core now has one bounded canonical container source tuple: `container_images`, `_container_exec`, `_container_launch`. The last member is a transitive shim dependency and is intentionally attested even though Core does not directly import it. Import-time source identities are re-read at sealing, normalized into the runtime body/hash, compared with loaded identities, and re-read at execution verification. Historical/minimal runtime bodies without the optional source fields retain their original normalization/hash behavior. This does not backfill container attestation into historical receipts.

The container helper imports also retain Core's standalone flat import convention. The private command runner was renamed `_run_container_command`, rather than weakening the duplication baseline. The proc-stat local was renamed `proc_fields`: it is positional Linux process metadata, not a closed wire policy.

Production packaging already selects every `src/prismabuild/*.py` file. Both `tools/` and `tools/fleet/` entry layouts resolve the generation-local package. No publisher module list or acceptance-fixture module list needed extension. No test source was edited.

## Reviewed wire maintenance

The runtime validator's allowed optional source fields changed, so the inventory was refreshed only with `tools/maintenance/refresh_compat_field_sets.py`, using `--before-source HEAD --review qualification_artifacts/runtime-closure-field-review.json --report qualification_artifacts/runtime-closure-field-refresh.json --apply`.

The checked-in review accounts for the removed digest-only expression and the added bounded container-source expression. The tool reported **before_count=572, after_count=572, reviewed=true, applied=true**. `tests/compat/field_sets.json` is tool-generated, not hand-edited. The companion report retains before/after source rows and the review. Early maintenance attempts timed out at 30 seconds and then hit a denied literal `/tmp` report location; the completed invocation used the permitted temporary directory and then the retained artifact paths.

## Completed checks

All tests were run through the admitted `run_tests` facility. Its environment reported Python **3.14.4**, Linux **x86_64**, **dl380g10**, and bounded CPU/thread resources.

### Main integration and guard selection: 698 passed, 273.49 seconds

Command:

```sh
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 python -m pytest \
  tests/test_core.py tests/test_wire_inventory.py \
  tests/test_publish_runtime.py tests/test_fleet_submit_layouts.py \
  tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py \
  tests/test_design_doc_line_references.py tests/test_client_sdk_surface.py \
  tests/test_client_sdk_fail_closed_edges.py \
  tests/test_container_class_requirements.py tests/test_container_image_inventory.py \
  tests/test_container_class_launch_acceptance.py \
  tests/test_container_absolute_client_execution_acceptance.py \
  -q --tb=short --basetemp="$TMPDIR/pytest"
```

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"1781a5fb09c32b3dc07a23f508f6a4d13c6206f97d1aa119e2d7982effe10a46","size":4177}},"operation":"5bd46f05f0c46818caec2f498bd29691179a5c7b3973e2b29a504a8b6fda3e1e","receipt":"3b3c7f5a03a7a8a6170c4f6d4e6a1ea3156d0610f8918972348b06c726888b53"}]

### Actual publication closure: both entry layouts passed

An inline qualification copied every member of the actual `_publication_manifest()` using `_source_for()` into a temporary generation outside the worktree. It asserted all three helper sources were included, then launched `tools/seal_and_publish.py` and `tools/fleet/seal_and_publish.py` in separate `python -I -S` processes from outside the checkout. Each process imported Core and all three helpers from that generation, checked runtime normalization, and verified the unmodified runtime. For each of the three helper sources it then appended a comment and independently required both sealing and execution verification to refuse it, restoring bytes between probes. Thus both layouts passed all six mutation refusals each. This was an inline check, not an added or edited repository test.

Observed output:

```text
PASS tools all three helper identities and six mutation refusals
PASS tools/fleet all three helper identities and six mutation refusals
PASS real publication manifest, both fleet layouts, isolated from checkout imports
```

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"7de984077f5754369fc342734704423a605afe8b7ebb2c3260fabf4c5d39e9b6","size":219}},"operation":"5ff3cf09e635b25956b8f779a931866b9f7c629362ac674e476d7484109da78d","receipt":"0158aad6c9ff514758157e765d8bb8af5f9b43cc091c1376b741dc6b08966522"}]

### Final inventory, duplication and design-reference guards: 17 passed

After the final design prose adjustment, reran `tests/test_wire_inventory.py`, `tests/test_duplication_baseline.py`, and `tests/test_design_doc_line_references.py` with the same pytest/thread/temp options: **17 passed in 97.22 seconds**.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"53dac0fbe078bf887ca97e3db638032bc3c76ec2983fdef06d230b122033df09","size":1506}},"operation":"6bbe7546c9e282d40c8b1a937c2509bc74c57145c05b74335c57247a707dfc6d","receipt":"e600a531a8a97942dc5d715127cd6535419d365481111b8b9f0916d296a1f7d3"}]

## Failed/intermediate checks and limitations

- An initial `pbtest.py` command was refused (exit 125): this facility already executes inside an admitted PrismaBuild action and disallows nested submission. Subsequent commands used direct pytest through `run_tests`, as instructed by the refusal. Operation: `125d5b58c69735a2bacd8bd28a982b3f1f96236ef2001e70d1dd7e19e3a09c82`.
- Intermediate targeted run: **145 passed, 1 failed** pending the mandated wire refresh (`c9651cbc6d35c2edaf4b880526945bb9b7835c0c9042aedd42edced26090417b`).
- Intermediate integration run: **481 passed, 1 failed** because absolute package imports in flat-Core fallbacks violated the existing independent-Core invariant. Replaced them with sibling imports and made the needed helper imports support that convention; the final invariant passes (`7691391b1cbb12699a7cc22ceee64feb7c2e07daf9d35fb7b8a99f712cea52a3`).
- One selection used nonexistent `tests/test_container_images.py`: pytest exited 4 with no tests run. Corrected by selecting the actual inventory and requirements suites (`907ef69829307000f1875f6addc34323d0ba09317bbc58124f3032f1c4a88b93`).
- The first inline packaging probe omitted the required `None` launcher argument to `_worker_runtime_identity`; it failed with TypeError before verifying the closure. The corrected completed probe above passed (`296ac7b28a6fe62cef02a073e016a9161d6858c18c5a4ee259c5520cb290e18d`).
- Evidence is for the reported x86_64 arm and selected suites only. No full-repository suite, AArch64 execution, production Docker daemon qualification, runtime activation, or fleet-wide rollout is claimed. Publication checks used a temporary copied generation, not the live runtime tree.
