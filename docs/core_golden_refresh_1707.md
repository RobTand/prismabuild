# Core golden refresh for #1707

Default activity supervision validates the sealed stall allowance in Core.
This changes Core from 305687 bytes with SHA-256
`403f6cf7f78112abc8d34582e7daea91d3c86bde55c2468a9e43fe9e48320483`
to 306962 bytes with SHA-256
`e285e2689e15a296a18170fe7997e4d2fb83c7e396990d18c13899d5fc6b5e6a`.
`digest_primitives.py`, `tools/prismabuild_worker.py` and `immutable_files.py`
retain their source bytes. Core retains its exact `_IMMUTABLE_FILES_SOURCE` pin.

The six current Core receipt, attestation and runtime records were refreshed
through admitted capture and strict runtime adoption. No golden or
`field_sets.json` was edited by hand. The importer accepts only Core source
size/hash changes and their dependent runtime, attestation and receipt digests.

Evidence is retained at
`/mnt/shared/pbtl-probe-tmp/pb-lead-dl380/1707-port/`:

- Capture action `f177751b304505317696767017540dfe8d726f18f695fe1445cd0b12d65d2e9d`
  binds committed producer `783e264394`. It independently reproduces six
  current and twelve historical records and passes both consumer directions.
- `runtime-primary-audit.json` verifies the capture's terminal record,
  source Git objects, request, logs, input blobs, receipt, result and cleanup.
- Adoption action `5c0f42afba714226f8f7e537cfc16d38fa18184d7eab944fa609a19b007937d1`
  runs `refresh_compat_runtime.py --profile runtime` inside PB. Its source
  parent `678ad475c2` differs from the producer only in port tests. Strict
  adoption verifies unchanged production and recorder sources against the
  producer. All twelve historical rows remain byte exact.
- `adoption-primary-audit.json` authenticates adoption and its CAS result.
  `integrate_verified_export.py` authenticates the complete five-file export
  manifest against that result before writing the checkout. It also checks
  unchanged unselected current records and all 129 prior historical/control
  files. The previous complete current files and provenance are preserved in
  `tests/compat/history-controls/superseded-runtime-7d899e510cc165ad/`.

Exact commands, client statuses, primary audits and integration results remain
beside the captures. Final namespace tests and repo guards qualify the adopted
corpus. This refresh changes source identity and grants no deployment credit.

The staged-read contract's ID-02 attempt identity, ID-08 runtime attribution
and SM-01 action lifecycle retain their existing owners. This change supplies
source support for default payload activity supervision; validation is scoped
to the recorded PB runs. Fleet deployment and live workload qualification are
not performed here, and the broader staged-read targets remain owed work.
