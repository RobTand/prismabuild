# Shared-source generations: implementation and qualification disposition

Release qualification remains **blocked**. The final repair section below
supersedes the earlier O5/O6 coverage claims: it records external surface-path
fallback and actual generated-shard inner-scratch retention, plus the still
unadopted Core receipt bytes. Focused passes are not full-suite acceptance.

This supersedes the earlier pre-implementation stop report. The operational
contract is in [shared_source_generations.md](../shared_source_generations.md).
Production W3/W4 support is now implemented; **release qualification remains
incomplete**. No golden, archival runtime declaration, or record corpus was
changed. No capture/adoption, rollout, performance, or HDD result is claimed.

## Implemented boundary and exact sharing coverage

W3 uses the existing Git population and core identity/ancestry owners, a
format-versioned complete-snapshot/CAS-context key, private construction, atomic
publication, and generation-specific kernel reader locks. A trusted local
publication binds the generation proof to an entire source/Git inode census.
Different complete snapshots cannot alias; changed bytes (even with restored
mtime), Git state, permissions, or uncertain census refuse reuse. Per-action
inputs, snapshot binding, closure and stamp/producer checks remain independent.

W4 is explicitly selected with `pbtest --source-readonly`, for compatible,
unprofiled host/pool shards only. Source cwd is separate from physical private
results, locks, recovery claims, temp/cache/helper/surface destinations, and CAS
ingestion. Worker paths are resolved after admission, not embedded in seals.
Unsupported selections retain private execution before shared creation. A
selected shared attempt is never privately replayed following a refusal.
Source and attempt descriptors are inherited by action descendants; failed,
interrupted, unacknowledged, or still-read attempt scratch is retained. Shared
sources do not enter legacy action-owned recovery/deletion or SLURM cleanup.

This is a **cooperative read-only contract**, not containment against hostile
same-UID code. Tests establish neither arbitrary-action sharing nor deployment
isolation. The implementation deliberately adds no storage backend, ZFS work,
attempt-retirement service, object cache, or performance qualification campaign.

Exact new acceptance coverage per arm:

- All **26 unchanged acceptance cases** pass (14 generation cases, 12 consumer
  cases, including two unopted private-execution controls).
- The concurrent generation test has two readers plus a third overlapping
  borrower: **one Git init, one checkout, one core identity computation**.
- The shard test runs **two concurrent generated pytest task subprocesses** with
  an injected pool-launcher adapter: one shared cwd/generation and **one init,
  one checkout, one identity computation**, distinct writable paths, action keys,
  results and receipts. It is not a live-fleet multiworker performance result.
- A real fresh-interpreter test borrows the published generation while its
  `git_checkout_identity` is patched to raise: adoption and per-action validation
  still work. A real duplicated lease descriptor also protects source after a
  launcher is SIGKILLed until its final descendant exits.
- Source-writing failure is observed once, without private replay; the next
  reader receives clean source. Private writable execution remains covered.
- Additional attempt-lifetime tests cover failure retention, inherited locks,
  surviving descendants, identity replacement, and conservative cleanup.

## Completed focused checks on both arms

All commands were plain pytest through `run_tests`, already inside admitted
fleet actions, with `--basetemp="$TMPDIR/pytest"`; no nested submission was used.
Arms were native x86_64/Python 3.14.4 and aarch64/Python 3.12.3, CPU-only.

### Final feature, recipe, lifetime and repo-wide guard command

```sh
python -m pytest tests/test_shared_snapshot_generations_2119.py tests/test_shared_pbtest_layout_2120.py tests/test_shared_pbtest_attempt_lifetime.py tests/test_shared_source_proof_lifetime.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py tests/test_client_sdk_surface.py tests/test_pbtest_source_readonly_recipe.py -q --basetemp="$TMPDIR/pytest"
```

**397 passed, 12 warnings on each arm**. Warnings concern the existing bounded
reader's multithreaded fork. Both immutable acceptance-file hashes are unchanged.
The wire/help/duplication/design-reference/SDK guards pass on both arms.

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"d02d9868a1d48ce3e3215f380a1e70f2637a33188d76e6b4253b5076fc10398b","size":3790}},"operation":"47fabc8782ebb0d11d3f1011f3de5ba92dfe302e9a620cb5cbc1ee9aa4850ca3","receipt":"9f12c17437f264de846d327f4abcf352be43678d340c691ce28b544620037dac"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"7d9eebb7019464f84e3da5bd14662f22cef8c733ee0cf32491195cd5dc7ca66f","size":3803}},"operation":"a879ca56c77648f933a29390621070a6f3e4607a49f365ad8b85369c13d968b0","receipt":"f633947cc4a0e11b7681a4c0408e28f1b82203ab5a13959b3c9499d3e6d950ed"}]

### Focused materializer, pool, scratch, capture and identity regressions

```sh
python -m pytest tests/test_shared_snapshot_generations_2119.py tests/test_shared_pbtest_layout_2120.py tests/test_shared_pbtest_attempt_lifetime.py tests/test_checkout_reaps_dead_siblings.py tests/test_pool.py tests/test_local_scratch_core_recipes.py tests/test_checkout_recovery_1465.py tests/test_pbtest_attempt_temp_1542.py tests/test_pbtest_attempt_temp_expiry.py tests/test_pbtest_scratch_roots_1710.py tests/test_capture_argv.py tests/test_sealed_output_capture.py tests/test_capture_preserves_directory_path_observations.py tests/test_default_transport_travels_with_the_generation.py tests/test_producer_snapshot_is_proven.py tests/test_checkout_identity_is_byte_derived.py tests/test_client_sdk_surface.py -q --basetemp="$TMPDIR/pytest"
```

**749 passed, 20 skipped, 14 warnings on each arm**. Skips are not passes. This
run preceded helper-only renames; the final feature/guard run above covers the
resulting source. No assertions, acceptance files, or discovery were weakened.

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"9d95d2682f6ca57663a74ffba7f5d9730987935d12ba984339b1aecdfd0955f1","size":5671}},"operation":"834871f650a033e9ca9f8f6ccae004edea0207c541b64d0aed50cabd3ab7a954","receipt":"dfe2bddea918703e8b0804db3714b2bf7ff407c459f46a43a6ec4023478d7946"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"85d4eefdd43a8a278ea06f16efcaca37293624051e5557780ed6d0f68d1c9bb2","size":5685}},"operation":"6a8afd33aac344845c7af19641e547531009a19dc09a2def1f4fcc4999ba3e8d","receipt":"9e8a1a6010f29bd5e0fe496e0b0c43ef95c1ae45b94fd4c31bfb6baaf28ea7e7"}]

## Full-suite attempts: incomplete, not qualified

The requested exact command `python -m pytest -q --basetemp="$TMPDIR/pytest"`
was attempted once per arm. Both ended **exit 124**, without pytest's final
summary: x86 last displayed 47%, ARM 76%. Both captures contain failure/error
markers, but no final node IDs/tracebacks. These failures are **unclassified**,
not asserted unrelated or harmless. Neither full suite passed. Repeating the
same timeout was not used to seek a preferred result.

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"6d794ff0ef30ba45b84a79cb153f2828f5dad6292c4f899e4dfa6aaf3b127038","size":47631}},"operation":"1377dc77cc33de957687f80ae485b8cf58798405062a8b5557fda86bb431f1ec","receipt":"eda3c50a1d9d70bdf00e140e19c3a6f503491ed2c519e44719177106465bff81"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"21c744c07ed7d7e4f6a2b64bff7f21a2b256ccaa266cc7d56f6716212d3f4733","size":57364}},"operation":"d7b8be30099cb6325204b716950517b614eba82f1032511410ce240a948d1852","receipt":"6cc745e9d011592e990249652eeac0fc17cf26756f315c81e3d471385971c2a5"}]

## Required record-compatibility disposition: stop for capture/adoption

The inherited archival prerequisite was corrected portably in its existing
owner: expected TMPDIR comes from the selected **pre-spawn child environment**,
not a hard-coded deployment directory or the child's response. Full strict
runtime/environment attestation remains intact. Before Core implementation,
the x86 Core preflight completed 73 cases; this is not final-change evidence.
ARM instead failed its first archival writer because its declared exact
`/home/rob/venvs/pb-cpu/bin/python3.14` executable is absent. The declaration pins
x86_64 CPython 3.14.4, not ARM's native 3.12. Substitution or weakened attestation
was not attempted; the existing archival qualification owner must supply a
supported lane/dependency or separately reviewed qualification.

ARM prerequisite evidence:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"56ba3bcf1f5c6d1e02fbf9c08aa8affe3307689c16711113f5fd35af9e3fa9e7","size":32528}},"operation":"9eec5ff0c6aeaa3b544af113086f06576282e32a122ae80cdf6e48a2e95fcd70","receipt":"35e29fcb99324c48e719b382ef6057207b202f6db567f5b80550b0b173dc87c6"}]

After implementation, this x86 namespace-gated command stopped with **1 failed,
19 passed** (controller cases were not reached):

```sh
python -m pytest tests/test_record_compatibility_core.py tests/test_record_compatibility_controller.py -x -q --basetemp="$TMPDIR/pytest"
```

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"8859de88cab43c824cf10ae94a1594935624eb6fda82b132e6959ade2b477430","size":1048576}},"operation":"236cc402c319f081a449800ad820ab4a34d735cc6ccc048f5e0558b83452fe41","receipt":"fdd2ffa05e0cd6a147c0c4baddffcd51d21dce992de2f7b2b845e105b4afd643"}]

The large retained capture was truncated, so a single diagnostic selector with
short traceback and capture suppression established the exact assertion:

```sh
python -m pytest tests/test_record_compatibility_core.py -k 'core-receipt and in-process' -q --tb=short --show-capture=no --basetemp="$TMPDIR/pytest"
```

Result: **1 failed, 72 deselected** at `tests/record_compat_contract.py:200`:
`AssertionError: current writer changed golden bytes`, case
`core-receipt/in-process`. This is a required capture/adoption gate, not a passing
compatibility result. Core receipt provenance includes the changed Core source;
no claim is made that its new bytes have been adopted.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"c7bdf13ae0251d4ddd790283c8cd2e906b92680d4102ee5a6c1ff70ff738ade1","size":48543}},"operation":"28023314142d40f76a7a134a1f29b06b219f666aa631ede37697da06cd8b7a06","receipt":"9c25400e391ce615d39b2e5a5e3760a606659e22025f6517bac2ed3841e2fd16"}]

**Stopped without editing goldens or attempting unsupported adoption.** The PB
release owner must perform admitted namespace-lane capture and strict runtime
adoption, then complete compatibility and broad qualification. No objective is
declared impossible merely because these release prerequisites remain pending.

## Reviewed field-set refresh

Used `tools/maintenance/refresh_compat_field_sets.py --before-source
bff154e8789435cf9b025f6b878ea692110e8bda --review <external-reviewed-json>
--apply`, not a hand edit. Exact reviewed deltas: replace the pool claim-scoped
field set with its previous members plus `execution_layout`; add the four
reserved worker path symbols; add the shared publication's exact
`census,generation` key set. The tool reported **572 -> 574 rows, reviewed=true,
applied=true**. Final wire guards pass on both arms. This AST inventory update
is not record-golden capture/adoption.

## Repair after review: surface selection and generated scratch lifetime

The prior synthetic lifetime cases did not prove the generated shard's own
cleanup boundary. Reproduction with a real transformed shard found pytest
returning zero but `final-verdict-evidence` already absent before the final
attempt outcome: **4 failed**. The earlier surface selection reproduction
likewise reached the resealer for absolute/parent destinations (3 fallback
assertions and 6 direct-transform refusals failed). That initial combined test
run also had four new lifetime-fixture call-signature errors; those fixture
errors were corrected before the separate real-shard reproduction below.
These were not feature acceptance-file failures or impossible objectives.

Surface-selection reproduction:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"b0831c0cadf7bc91511bd4f6ee552ae2a372e318e81257c18e314c0c49dfcedf","size":9322}},"operation":"03f75bdd1e90823bbb47e8a77fc89ae27cefcc83533966606c25eed981ae14fd","receipt":"9cf24da8c9b6c2c7f82e73b1f120cd3a74a3a64cd36ec74b67b2491487a6c621"}]

Actual generated-shard lifetime reproduction:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"265f40e5968ea214d9cb41b904170a323b6443c798a5824d63446e7f51b8d51d","size":3407}},"operation":"0f2e5ce3ea4b2dac8a304a4d5e5e3efccb98e019866a2975ea4449e1be71816e","receipt":"0eb1502122013054b4a386a5951fe84443dc7ce544e5b31c2924ae099e9c6090"}]

### Repairs and their limits

- Check every surface option in both original request and sealed command before
  resealing. Absolute paths (including submitter-checkout paths) and any `..`
  component retain the private recipe; the transform also refuses them. Private
  submission still enforces its existing portability/path-binding rules. This
  is not a guarantee that external destinations are available on other workers.
  Supported relative report names retain the documented opt-in attempt-local
  semantics and are not promised as persistent external artifacts.
- Only the shared resealing transform replaces the standalone attempt-temp
  program. The new carried program creates inner pytest scratch under the outer
  attempt without independent registration, sweep or successful-return cleanup.
  The private program bytes and private 24-hour expiry contract are unchanged.
  The outer attempt removes all scratch only after its acknowledged-success and
  exclusive-reader gate, using the existing inode-checked owned-tree remover for
  read-only directories too. Failed, unacknowledged and still-read attempts keep
  the inner data pytest itself retained. Pytest/test/plugin cleanup remains
  independent; PB cannot restore files pytest has already deleted.
- Generated-shard tests execute pytest and then supply final attempt outcomes:
  failed, unacknowledged, successful-with-reader, and drained success. They
  verify the nested marker before the final verdict and after context exit,
  and refuse standalone register/sweep calls. Existing pool tests separately
  exercise final source/resource verdict ordering. This is not a claim of a new
  live-broker end-to-end retention or durable queue-status publication probe.
- Private real generated-shard controls in both surface-option spellings leave
  their requested external report intact after temporary scratch removal.
  Publication-selection tests additionally cover original-external to
  rebound-relative and original-relative to sealed-external combinations.

### Both-arm focused verification

Ran the union of all O1–O6 named files plus the generation-proof, recipe and
attempt-lifetime regressions with plain pytest, `-q --tb=short
--basetemp="$TMPDIR/pytest"`: **937 passed, 20 skipped, 14 warnings on each arm**
(x86 Python 3.14; ARM Python 3.12). This includes the unchanged acceptance files,
legacy reaping/recovery/transport controls, producer/byte-derived proof checks,
all four repo guards and the SDK surface guard. The source-sharing assertions
still exercise **two overlapping real shards on one snapshot, one Git init,
one checkout and one expensive checkout identity**, with distinct writable
paths/actions/results/receipts. No larger shard count or performance improvement
was measured.

x86 union:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"ff69c8da0c227192ee869fb899366dc37a4f26fb3a9eb9e5472669438a43c76c","size":7074}},"operation":"bf94eb57938d276ce6e4262be23c7b9ac437ae75e4960e24327eb8eee27279da","receipt":"67530238875a9abdbfd6b727f87e24981beecb4571c63058ac549e1c0df88ce6"}]

ARM union:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"3db0627fb5a736ec1e188906c24de2030451450ce4553f4e3fd9dd986a588bff","size":7088}},"operation":"6b34ae3a0addbcf12f6ffa0085e39813c51b566e747de11e031bb7afbc20ba2b","receipt":"469cca9d36717ebb34d0e03d2c622ec4c05ef3cc343143f66a4d551dbf4cc7df"}]

After adding the two original-versus-sealed path cases and updating help/docs,
reran `tests/test_pbtest_source_readonly_recipe.py`,
`tests/test_shared_pbtest_attempt_lifetime.py`, `tests/test_wire_inventory.py`,
`tests/test_fleet_tool_flags_have_help.py`, `tests/test_duplication_baseline.py`,
`tests/test_design_doc_line_references.py`, and `tests/test_client_sdk_surface.py`
with the same pytest options: **385 passed on each arm**. These counts overlap
the union; they must not be summed. No field-set delta was introduced by this
repair, and neither the inventory nor a record golden was edited.

x86 final subset:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"8bed56e7482e63ec427edc37d47767c9c2d2dd42a04e1ae267e213b196dab6d3","size":2705}},"operation":"fdf40c4b3a7d58137a595c4d01361159c5d7cb31ac3a1d17d686ef88daac2793","receipt":"98b2e911fb24fb6860db110eee55efe721b7334218e2b187ac6f9f5f582bc478"}]

ARM final subset:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"e092396ed831ebe2355f1b2aacd7cc0bd009ce671c6bf33912582ab848ff40ec","size":2707}},"operation":"8b4846e7fa37ca1706e6dc302c0c4956e41556abc961da7a06d0984495e4e786","receipt":"141b9bcedc11b19298b942fd590f3ab618d7ca32d38a93c7ad109632f6dec40c"}]

### Required release stop, not passing compatibility

Rechecked on both supplied arms:

```sh
python -m pytest tests/test_record_compatibility_core.py -k 'core-receipt and in-process' -q --tb=short --show-capture=no --basetemp="$TMPDIR/pytest"
```

Both returned **1 failed, 72 deselected**, at
`tests/record_compat_contract.py:200`: `current writer changed golden bytes`,
`core-receipt/in-process`. Retained stdout was read to verify that exact refusal.
This is the change-induced, unadopted production-writer provenance delta, not a
pass or an unrelated environmental failure. No capture/strict adoption was
attempted in this check environment, and no golden was edited. The PB release
owner must complete the admitted namespace-lane capture and strict runtime
adoption, followed by compatibility and full-suite qualification, before
publication. **Release qualification stops here.** Full suites were not repeated
in this repair after the known capture/adoption stop; neither the earlier partial
runs nor these focused passes establish full-suite success. No live fleet,
storage-backend, universal-sharing or HDD-performance result is claimed.

x86 release refusal:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"80ac56d82198cfeee3160edd785feb3513d2840118def13b2b520dc993c28ff9","size":48544}},"operation":"c9a603dcac8b08befbdbac3839df2eb328abd0bf793feb2f1ae7c9a1a05648fb","receipt":"abbdafa69f18b321f4559911cb4fb7b98a182285f0a943645804fc40e68835e9"}]

ARM release refusal:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"e5329bf37f29ea28fc1ba4bf1357fa9c4fdd44dac65e3ad566e6fd7026375958","size":48679}},"operation":"f8c10aa654f345b087daa8e7249002079c0b342e5a6c9be3c075ddf6791f3a8b","receipt":"510cc4894c51779847a573741e76496955dc51753d370d0245c01d1eaeafdeef"}]
