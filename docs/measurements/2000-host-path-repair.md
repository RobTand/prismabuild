# #2000 / PR #2009 repair verification

## Scope and baseline provenance

The reported failure was reproduced on PR head `32e99cac1d019ed3d70918f491c66ebbdc8436fe`:
the ledger file had 3 passes and 7 setup errors because its pinned historical
`git show` exited 128 in the admitted checkout. This repair changes only that
file's historical-source loading and adds a no-subprocess regression. The other
seven audited files, socket limits/skips, locality fixtures, identity diagnostic,
behavioral assertions, and production code are unchanged.

Reuse the already-committed compatibility archive through `record_compat.retained()`
(which verifies archive, receipt, and member hashes). Select the exact generation,
not its position in the retained window, and still compile only the three selected
functions. There is no Git or current-code fallback. This is the narrow ledger
repair needed for this review, not the repository-wide migration in #1999/#2006/#2008.

Read-only source inspection in the editing checkout compared `ast.get_source_segment`
for `_read_commitments`, `_write_commitments`, and `_class_sums` in the old
`d2f92f5ff533489182dddaf286feb54afde6f34a:src/prismabuild/produced_output.py`
against the retained member. All three matched **verbatim**; the full modules differ.
No test performs this historical Git read.

| Input | SHA-256 |
| --- | --- |
| Old full module | `ab95d61dbceeb8dcd50c8d4aa99ad5c9f004bef404268a30eb75fd63158e3157` |
| `tests/compat/generations/a17477b2b206-1791745175-65dffa700dfb.tar.gz` | `151e169f7d2abf53168fb195ee063128ca29c2e9b7c5a9857eb383621d81f0c7` |
| Retained `src/prismabuild/produced_output.py` | `8d636899e500ee8ea88dde1b5654a1a938fba619b76fb485ac17903ec0898187` |
| `_read_commitments` source segment (both inputs) | `9d42d4d2ae92c9078622bf0a5367c4781a27d6503e0579012532337a7c0df279` |
| `_write_commitments` source segment (both inputs) | `c96ee20b3cc2cf206d92a9aa76929bfbf949a6a7f87fb383ec7172a3461d1ec4` |
| `_class_sums` source segment (both inputs) | `6ec6a5e7d1d10f724b9caf898801fae44a8f279d46287b1eaa70d5d74e4489ce` |

## Admitted checks

All commands ran through `run_tests`, without nested submission, on the supplied
x86/Python 3.14.4 arm (2 CPUs, 4 GiB). Each set native-thread limits to 1.
The first request with `arm="x86"` was not executed because the tool exposes only
its default arm; subsequent requests omitted the arm. Retained stdout was read to
verify the historical error, final results, source identities, and skip reasons.

### Reproduction and focused repair

Before and after command:

```sh
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 python -m pytest tests/test_batch_ledger_ownership.py -q --basetemp="$TMPDIR/pytest"
```

Before: **3 passed, 7 errors**, exit 1. After: **11 passed**, exit 0, including
the new regression that loads the original functions with `subprocess.Popen`
patched to reject every subprocess.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"12937e061683cbdc0b1435915fe55e077cef0e35c0a892931e67c42d27d300e8","size":35588}},"operation":"79c560ffe72e1d50cd8bc6c362e1a4b4bda94532d73b7b78142fcf00abd239d4","receipt":"5da37972217b804218d76915bd734c332454fdd9ac5f4159d55fb5589b4f80a7"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"58f473ff4248b28707f5b62cc91089daf7bc396ee09f21370b90902a3eecd398","size":1563}},"operation":"e95a2edd7dcb59fc96eff9edb1022fb261c0dd989c740460deb3445a1211f611","receipt":"8dd39e628660a2b587d5d0acc9a76bca5d0446a8635b35b74c857b307653fe32"}]

### Complete eight-file check

```sh
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 python -m pytest tests/test_an_unknown_interpreter_keeps_the_image_reason_1595.py tests/test_batch_ledger_ownership.py tests/test_pbrun_cross_box_cwd.py tests/test_pbrun_placement.py tests/test_pbtest_attempt_temp_1542.py tests/test_pbtest_sealed_basetemp_1469.py tests/test_placement_can_open_the_runtime_it_seals.py tests/test_worker_loop_offers_its_affinity.py -q -rs --basetemp="$TMPDIR/pytest"
```

**408 passed, 2 skipped**, exit 0. Both real-shard socket checks skipped with
measured **123-byte** resolved paths against the unchanged 100-byte bound.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"d30b7f28268382663ee9043c36b80406a6593e2b41aa821749b4e5fb7c0fb0a7","size":3912}},"operation":"4a4939ccde73c1b5c737f12aa87357e3aa261e9df09100012a970c924c0ededc","receipt":"3633c4523bde6b2a7c94e48c153f5f067e1c8f6c416ec33f8016eb1308210fc3"}]

### Deeper run-owned scratch plus inventory checks

```sh
export TMPDIR="$TMPDIR/deep/private/socket-fixture-regression"; mkdir -p "$TMPDIR" && OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 python -m pytest tests/test_an_unknown_interpreter_keeps_the_image_reason_1595.py tests/test_batch_ledger_ownership.py tests/test_pbrun_cross_box_cwd.py tests/test_pbrun_placement.py tests/test_pbtest_attempt_temp_1542.py tests/test_pbtest_sealed_basetemp_1469.py tests/test_placement_can_open_the_runtime_it_seals.py tests/test_worker_loop_offers_its_affinity.py tests/test_duplication_baseline.py tests/test_wire_inventory.py -q -rs --basetemp="$TMPDIR/pytest"
```

**424 passed, 2 skipped**, exit 0. Both measured real-shard skips were **162 bytes**.
Both prefix socket binds, scratch containment, tempfile-cache preservation, and
all executed cleanup assertions passed. No inventory refresh was needed.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"c34d375a6da9cc6cfd6e3a483ed4005e59b2d7a3028234e868c1fb6cbb28dd54","size":4380}},"operation":"9f4d9818aaf8c3575694c370a894ddf7a54cd11eddf375c2385f1ac54a7ba6e3","receipt":"e4ea2964f2312b96d1642baa7f4d7d805fc7461d34e57b0c3ecb2b3135d605fc"}]

## Limits

No ARM arm is supplied. No full-suite, NFS/shared-root, or short-root real-shard
bind execution was performed here. The existing successful short-root/ARM release
gate claims were not independently reverified. The two real-shard skips above
are not passes or verification of their socket binds/cleanup. No changes address
the separately tracked NFS cleanup issue #2003.
