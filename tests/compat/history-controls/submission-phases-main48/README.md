# Submission-phase supersession compatibility control (#1936)

`main48-48c47c054048.tar.gz` retains the actual historical source used by
`test_timed_supersession_preserves_both_reader_directions`. The supported
runtime archives predate publication endings, so they cannot exercise this
record's historical reader and writer.

- Commit: `48c47c0540483461201dc8b5f8996976390fdd9d`
- Tree: `b757f56bb49e601e7bbe8b9abf5d281a0e3ce1f9`
- Archive SHA-256: `635d5ac00c7abcff6511d4bca81d907f759208883e203328ec9573f71a7f1715`
- Embedded `COMPAT_SOURCE.json` SHA-256:
  `6d252b40161a7a11d2c6674958cdfd2d33413e07305d9a925d2a89cc2342350b`

This is an **unpublished Git source control**, not a supported worker generation,
publication receipt, or designated rollback target. The archive contains the
302 regular files under `src`, `tools`, and `fleet` at that commit, plus the
source provenance document. These are the existing prepared-source capture
tool's complete subtrees, including import and data siblings; no candidate
modules are substituted into the historical closure.

## Preparation

The existing capture tool produced the archive, per-file manifest and index:

```sh
python tests/compat/support/compat_sources.py prepared \
  --root . --commit 48c47c0540483461201dc8b5f8996976390fdd9d \
  --name main48-48c47c054048 \
  --out tests/compat/history-controls/submission-phases-main48
```

Only fixture preparation needs that Git object. Test execution uses committed
inputs and never archives, fetches, or skips for missing history. The existing
`retained(..., history_control=True)` loader verifies the archive digest, member
inventory, embedded provenance digest, commit and all source hashes before
using a temporary extracted closure. Both historical operations pass the
manifest to the existing isolated compatibility driver, which verifies the
executed production sources. The test retains the exact five-field and golden
byte assertions, actual historical-writer assertion, and both reader directions.

## Verification

The focused repair check is:

```sh
python -m pytest tests/test_record_compatibility_queue.py -q \
  -k 'submission_phase_fields or timed_supersession' --basetemp="$TMPDIR/pytest"
```

On the admitted x86 / Python 3.14 environment, before this repair it reproduced
`fatal: not a tree object` for the pinned commit: **3 passed, 1 failed, 181
deselected**. After retaining the fixture, the same command passed **4 tests,
181 deselected**, including the actual historical writer and both reader
directions. No skip or alternate decoder was introduced.

RED evidence:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"b26a6a517c055a7d88c3946b9bcffcb29b6ee8e9664266b6069d21508084625b","size":7674}},"operation":"fa6394104e1bc0676c82bfaeb65fc0da98acd6f7b2e9c5effb9032a26b961193","receipt":"a97589e4425a8833db8a1a3089f67e7de80d1c115baa81aa5856c7dc2d7f0c0c"}]

GREEN evidence:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"b2ce8f6bb04fa9317755be126b5abc56fe2ac3ff181355653305f12f9aaa12a6","size":1431}},"operation":"28e7eabcea20b3832c1b7328b5ffee8bdaa19e66ea5b708bdd131deba20d91c4","receipt":"5f126da4991f2a07e55bb3bec76b649ddebc1fdffbeb87e349a6c55372613107"}]

Timing, event, consumer, sealed-publication and lock regression check:

```sh
python -m pytest tests/test_submission_phases.py tests/test_submission_phase_review.py \
  tests/test_pbrun_client_events.py tests/test_consumer_surface.py \
  tests/test_pbrun_sealed_publication.py tests/test_posix_lock.py \
  tests/test_pool_transition_lock.py tests/test_retired_lock_files_keep_exclusion.py \
  -q --basetemp="$TMPDIR/pytest"
```

Result: **193 passed**, with 67 multithreaded-fork deprecation warnings from
`_bounded_reader.py`. The retained stdout for all three operations was read back
and verified. The full queue suite, release gate, ARM lane and live TL timeout
integration were not rerun for this test-input-only repair.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"71667b432077d09d238647bcf70208ea3b3d266707941478980a5b956e5226a1","size":3119}},"operation":"8633532f99a605afafd6f8b79ab038657485ede70be7d534093fd9ccb7d36ddc","receipt":"650b6b2093ae2b97bada449d4a8f9b5f422084c4d59629fcfde9c031bf191726"}]
