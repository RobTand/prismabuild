# #2099: reviewed integration closure preflight (opt-in)

This separate adapter closes input binding only. It preserves `run_activation.py`,
its accepted component tests and historical bindings. It neither runs selected
APIs nor proves compatibility, lifecycle behavior (#2100), campaign acceptance
(#2101), or parent #725 full-path completion. A ready result always retains
`complete:false` and `behavioral_red:false`. Ordinary PB discovery stays `tests/`;
PQ is not added to it.

## Portable delivery

Copy `binding_preflight.template.json` into a private closure directory as
`binding.json`. It is intentionally **unqualified**: null API/default/review and
generation fields and empty inventories mean unavailable evidence, not approval.
Historical identity fields are a starting point for review, not evidence that
this expanded selection has been reviewed. Do not fill gaps with test fixture
symbols, synthetic receipts, current-main defaults, or invented digests.

In an already admitted action, use the configured interpreter (no install,
private-interpreter assumption, or nested submission):

```sh
python integration/pb725/binding_preflight.py --binding 'closure with spaces/binding.json' --output "$TMPDIR/preflight.json"
```

All closure reference paths resolve against the binding document's directory,
not cwd. Deliver reviewed sources, input operands, prerequisite receipts and
review bytes together at those relative paths. Relocation needs no Git history,
archive, download, or API execution. Output location is explicit. Ready exits 0;
missing binding, incomplete selection, absent/changed bytes, invalid review or
installed-verifier refusal retains an `unqualified` JSON result with a named
setup `reason` and exits 2. These are never behavioral RED. Failure to write the
requested output is an I/O error, not a retained result.

## JSON contract and provisioning order

The binding schema is `prismabuild.pb725.binding_preflight.v1`, issue `725`.
`selection` contains:

- `pq.commit`, `pb_tools.commit`, `harness_baseline`, `pb_installed_commit`,
  `pb_sdk_version`, `tessera_commit`: exact source/dependency identities;
- `dependency_generation`: `pb_commit`, `sdk_version` matching the installed
  selection, plus a reviewed generation `identity`;
- `apis`: five roles `producer`, `source`, `render`, `wire`, `activation`, each
  with `file` (an inventory key), exact `symbol`, and explicit `defaults` object
  (empty only where genuinely applicable);
- `operands`: actual selected test paths, names, sizes/shapes, formats and
  explicit overrides, not merely a generic fixture label;
- `files`: named source/input references `{ "path": "...", "sha256": "..." }`;
- `prerequisites`: named receipt references with `path`, raw `sha256`, and exact
  `action_key`. Receipt JSON must name that same action; hashing alone is not
  independent verification of its historical execution or custody;
- `continuation`: source commit, paths, artifact references, issues `[859,864]`,
  `verification:"source-attributed-only"`, `complete:false`.

Provision and independently review the real closure first. Compute raw file
hashes with existing `prismabuild.digest_primitives.raw_sha256`, and canonical
selection digest with `canonical_sha256(selection)`. The external review receipt
has schema `prismabuild.pb725.binding_review.v1`,
`scope:"integration-binding-only"`, and `selection_sha256`. Set top-level
`review` to its `{path, sha256}` reference. Any selection change (including
pair, SDK, API flags, operands or receipt identity) needs new review evidence.
The receipt hash and entire canonical selection must match before installed
verification. This is an integration closure reference format, **not a new
provenance/approval authority or signature scheme**. Genuine approval and truthful
historical attribution still require independent review (O5); self-authored JSON
and successful commands cannot establish them.

Only after all referenced bytes match does the adapter invoke
`tools/fleet/pbtest_pins.py:verify_install("prismabuild", reviewed_commit)`.
The CLI uses that same verifier, with no identity-policy waiver. Git identity,
RECORD bytes, import ownership and unrecorded files remain owned by that verifier;
its `installed_commit`, `origin`, `verified_files` and other returned evidence
are retained unchanged under `installed`. The adapter neither copies those
checks nor mixes package generations. SDK generation is a reviewed selection
consistency check here, not an executed compatibility probe.

## Real source observations, not new execution evidence

The existing `README.md`, `activation_binding.json` and
`source_render_binding.json` attribute the approved historical PQ
`a3f8d01a4fdb26cfa81c5f6879ae5b17af367c89`, behavioral PB/tools
`059953bc3793f539600d333cd3311773e592b0e6` SDK1, and Tessera
`b40c93cb73745097e57a1ba4cf5b9eee166c759a`. Harness/verifier baseline
`be674a35f30da209075e237a9d380afb857c05c2` is not the behavioral dependency.
The historical component source/render fixture produces safetensors source
inputs and CPU BF16 rendered weights with `render_production_weight` and
`_store_rendered_weight_entry` before staging. Its source selection uses
`source_read_plan.selection_spans`; rendered operands are whole-file, offset 0.
Historical operands include shape `[8,4]`, 32 elements, `max_file_bytes:65664`,
`lru_max_bytes:256`, `prefetch_max_workers:1`, and covered/omitted names recorded
in the binding. BF16 rendered cache artifacts are not Tessera encoded wire.
The tests explicitly activate `staged_tier_policy_test_context(DEFAULT_ALLOWED_TIERS)`;
that is not default production activation.

Source observations from served PQ `6d8288adc94b3c4b624c143ed372fb6513f32784`
are **not substitutions for the historical API binding**:

- `layer_streaming._source_safe_open(path, *, source_authentication=None, **kwargs)`;
- `production_weight_cache.render_production_weight` requires `qname`,
  `activations`, `levers`; optional rendering controls default to `None`;
  cache `prefetch(keys=None, max_workers=4, *, executor=None)` differs from
  the fixture's explicit one-worker override;
- `perturbed_x_cache.write_exact_activation_cache_entry` requires identity and
  tensor/file bounds; `release_file_pages=True`, `preallocate=False`,
  `durable=True`. `prefetch_exact_activation_cache_entries` requires
  `max_tensor_bytes` and `expected_session`, with optional resolver/residency
  arguments defaulting to `None` and `release_file_pages=True`;
- `staged_tier_policy.DEFAULT_ALLOWED_TIERS` is `"ram,ssd"`, but policy is
  inactive by default outside campaign scope;
- `tessera_producer_interface` names `src/tessera/export_serving.py`; authority
  flags depend on the checkout's packaged runtime contract. `tessera_render`
  derives memory/group/half defaults from Tessera, not independent PQ constants.

The supplied current PQ dependency observation is PB
`027103d9a8417e06c7f13356e58779a313cd7088` SDK5 (also named by
`tests/pb_runtime_generation_pin.json`); served PB declares SDK6. Neither source
observation is executed compatibility evidence, expanded-pair approval, or
permission to probe, shim, downgrade or select a replacement pair. Real expanded
API/default/operand review and its receipt remain unavailable in this delivery.

## #859/#864 continuation attribution

Supplied source attribution says PQ #859/#864 introduced source-bound component
checks at `26ae2f4c66163d5317c2c072b3052de1c1c59d85`, with
`tests/fleet_acceptance_pins.py`, `tests/fleet_acceptance_runner.py`,
`tests/fleet_acceptance_matrix.json` and historical
`tools/fleet_qualification_driver.py`. The latter was retired by
`eb9a20354fb0bc5468183867a4685a30f22d8376`; it is not a current entrypoint.
Source references name `pqfleet-r7red-20260920.json` and
`pqfleet-r8green-final-20260920.json`. The separate historical component candidate
`c5012fbfc6986c551af5606ff9d6f0d874768116` is not PQ's application dependency.
Historical machine paths are attribution, never portable defaults. This delivery
has not independently verified retained CAS objects, original component
executions or live installed environments. Continuation remains
source-attributed-only and `complete:false`.

## Portable verification

Explicitly select the new synthetic contract file plus repository guards:

```sh
python -m pytest integration/pb725/test_binding_preflight.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

Synthetic installed-distribution discovery calls the real verifier without
installation or PQ import. Those fixtures do not approve real symbols or review
receipts. Legacy component tests are not rerun: their historical prerequisites
are outside this environment. No full-path or campaign acceptance is claimed.

Observed portable result: 132 passed on x86 Python 3.14 and 132 passed on ARM
Python 3.12 (23 adapter cases plus 109 repository guards on each arm).

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"7af8edd007c6d5f6373058fa34bd4ca6aeabc1457f11d8b86c8fff4148cf9f14","size":1997}},"operation":"1427096742cfebd113805cedbbd11dc6935a50fd8d9e3e90f64808d9c75c4d7a","receipt":"124df0f923b25a7c2d66ce2fd7bbaeda0cf39ac2edbe0e3a2cd0ad2cad4b828c"}]
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"79defa6177b600382b02c14b357adbf3324726162a19315b85e7ca11d15285ff","size":1999}},"operation":"e7e5738938866a39e3084232c8a6bb31eb989202c025f2462680cca973ed4f80","receipt":"b6b67964829d5eb55f07883270cb5179509c6ccd8242d5430869b81dbb33f2bb"}]
