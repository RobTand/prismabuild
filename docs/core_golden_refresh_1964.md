# Core golden refresh for #1964

Merging #1936 into #1964 combines submission timing with bounded verification
of oversized ending logs. The merged immutable-file helper changes Core's
embedded source pin. Main's six current Core goldens consequently fail with
`current writer changed golden bytes`, covering `core-receipt`,
`core-attestation` and `core-runtime`, each in its in-process and script variants.

The strict runtime profile changes only Core's actual source hash and the
dependent runtime, attestation and receipt digests. Both source versions are
305687 bytes. Core's SHA-256 changes from
`403f6cf7f78112abc8d34582e7daea91d3c86bde55c2468a9e43fe9e48320483`
to `d398c3541cdc26e808191dd98d1674bd9c9c3a33c320967ca4b9a3323ad5e461`.
No record member set changes, and the digest owner retains its identity.

Evidence is retained at
`/mnt/shared/pbtl-probe-tmp/pb-lead-dl380/1964/round3/`:

- RED action `a2d96cfcffece2c92030f4f3f7e74cc2ede5500f92385fdb9df6129c4c4755ea`
  fails in exactly those six cases on the untouched merge.
- Capture action `4ce59ab9f7f13acfa4adcdb1a15240ad2b0144cfe76a4dca3daa8749d2c0a1fb`
  binds clean merged producer `bda776ed5a00e5aa6b524c07e67fe77c4e8b5bd8`.
  It independently reproduces six current and twelve historical rows and
  passes both real consumer directions, with no absent participants.
- `runtime-primary-audit.json` verifies the capture request, Git objects,
  captured logs, input blobs, execution receipt, result and scope cleanup.
- Adoption action `4a6c91715596ddd440ec86babb0ee9a77a3887144168d798a5226029170c486c`
  runs `refresh_compat_runtime.py --profile runtime` inside PB.
  Its strict report confirms that twelve historical rows remain byte exact.
  It archives main's complete previous current files under
  `tests/compat/history-controls/superseded-runtime-7d899e510cc165ad/`.
- `adoption-primary-audit.json` authenticates the adopter and its CAS result.
  `integration-verification.json` binds all five exported files to the
  manifest in that verified result before integration. It also verifies
  all 129 historical and existing control files, including main's
  `superseded-runtime-0c20614e4325b088/`, remain byte exact.

The source-only field-set scanner finds zero policy deltas from merged main
`81e4875085`. All 569 parsed policies remain pinned, with no metadata rewrite.
`RUNS.md` retains exact commands, action keys and counts for native x86 and ARM
validation and all eight record-compatibility files in the x86 namespace lane.
Archival replay pins an x86_64 Python 3.14 executable, so this refresh claims
no ARM archival replay qualification. Validation uses the committed refresh.
This capture qualifies corpus bytes and grants no deployment credit.
