# Historical pbtest reader for issue #1842

`pbtest_outcomes.py` is the complete, byte-for-byte historical source used by
`test_pbtest_record_binding_1842.py` and `test_pbtest_record_privacy_1842.py`.
It is a test artifact, not an alternate production implementation. Do not edit
or regenerate it from the current production reader.

## Authentic source provenance

Extracted from the original Git object available in the coordinator repository:

- Repository: `https://github.com/RobTand/prismabuild.git`
- Revision: `e7d49ad28712683547a42bebe9fd26ccea61178f`
- Revision subject: `Merge pull request #1978 from RobTand/tl/release-90f017067ba95cd0866ac11d5812e84375a97346d58b00d1e991b3cc843f1ac2`
- Original path: `tools/fleet/pbtest_outcomes.py`
- Git blob (SHA-1): `298c4bf93bfed977fcffdd0d3c4b570a7296e76e`
- Length: **53,715 bytes**, **1,101 lines**
- SHA-256: `495db65c9c29a565e65a5c03ed5e0406500cf48e801678ef66528a64d5ca9291`

Source extraction (maintenance only, from a repository retaining that revision):

```sh
git show e7d49ad28712683547a42bebe9fd26ccea61178f:tools/fleet/pbtest_outcomes.py > tests/fixtures/pbtest-reader-before-1842/pbtest_outcomes.py
git hash-object tests/fixtures/pbtest-reader-before-1842/pbtest_outcomes.py
sha256sum tests/fixtures/pbtest-reader-before-1842/pbtest_outcomes.py
```

The extracted file's Git blob identity was verified against the original tree,
not merely against a newly calculated checksum. No historical code was invented,
rewritten, trimmed or adapted.

## Loading and compatibility scope

The shared `legacy_reader_before_1842` pytest fixture in the binding test reads
these checked-in bytes relative to `__file__`, checks their length and SHA-256,
and executes them in a separate module namespace. The privacy test imports the
same fixture. Missing or changed bytes fail; there is no skip, Git/network
lookup, environment-specific artifact path, or fallback to current source.

The full original module retains its original imports of the checkout's
`prismabuild.digest_primitives` and `prismabuild.pbtest_collection`, as did the
previous Git-based loaders. Those imports support the recorder; the historical
reader/judgement logic is in this unchanged file. Tests call the historical
reader API, not its pytest recorder entrypoint (`main`).

The assertions still check both directions of compatibility: a new producer's
records are accepted by the authentic old reader (including the full-suite
consumer), and the current reader's judgement of legacy output with nested new
preflight records exactly matches the old reader's judgement. Existing binding
and privacy regression assertions remain in place.

## Repair evidence

All pytest execution uses `run_tests` inside its admitted action, with plain
`python -m pytest` and temporary files outside the worktree.

After the repair, both complete assigned test modules passed: **25 passed in
6.34s**, exit 0, no skips. This includes the four previously failing compatibility
cases in the admitted checkout, without relying on the coordinator's history.

```sh
python -m pytest -q --basetemp="$TMPDIR/pytest" tests/test_pbtest_record_privacy_1842.py tests/test_pbtest_record_binding_1842.py
```

```text
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"7ed62cc075d2a78791dd4e5b10a057d3763fb123e0a15a6f59152b80afb4f690","size":1347}},"operation":"28e2fb15cf18118cae8114ed53ff064b6dbc263278e3864b71b2abaad0700a93","receipt":"7833367699dc61e6b4b716f13151ba5fe3c9f30ed2008cbe095870ca3170e1d5"}]
```

Before the repair, the focused old-reader and legacy-preflight cases produced
**4 failures**, all at `git show` with exit 128: the historical path/object was
not available in the admitted checkout.

```text
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"f727a76f30263a5a6e1edee69b477378991ba5d1056c35b593f69ff43b63e830","size":26451}},"operation":"a6e37e9438112307725ddcffc9e938b8a3fecfdfd054dafd60f456e0b69b26db","receipt":"381ae1dd612c5cc81c05657c4742afa1c79535073defdac5b8c6ef8c1a027eb5"}]
```
