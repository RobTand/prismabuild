# Core golden refresh for #1936

Submission timing adds an inert observer to Core and instruments its existing
CAS boundaries. This changes Core's attested source identity. The namespace
lane consequently reports six current-writer golden mismatches, covering
`core-receipt`, `core-attestation` and `core-runtime`, each in its in-process
and script variants.

The strict runtime profile accepts only actual Core source size/hash changes
and their dependent runtime, attestation and receipt digests. No record member
set changes. Core changes from 304897 bytes with SHA-256
`4fc1a4eb669c7707f5be7b49293a0faaeba4d6c29a2a450932cb878e228de3fd`
to 305687 bytes with SHA-256
`403f6cf7f78112abc8d34582e7daea91d3c86bde55c2468a9e43fe9e48320483`.
The digest and immutable-file owners retain their identities.

Evidence is retained at
`/mnt/shared/pbtl-probe-tmp/pb-lead-dl380/1936/`:

- Capture action `6a5c8fc6d5e900e3962b82523d23a4f95982bddab2950b64bb3f3250b257ae01`
  binds clean producer `5d57f01a6e5ae79482086e914b25705e29ee609e`.
  It independently reproduces all six current and twelve historical records
  and passes both real consumer directions.
- `runtime-primary-audit.json` verifies the action request, Git objects,
  captured logs, input blobs, execution receipt, result and scope cleanup.
- Adoption action `064dff2de4cbff49fee7f93a4549193870de3059a3cd08e30a2f039d75be45f7`
  runs `refresh_compat_runtime.py --profile runtime` inside PB.
  Its strict report confirms that all twelve historical rows remain byte exact.
  It archives the complete previous current files under
  `tests/compat/history-controls/superseded-runtime-0c20614e4325b088/`.
- `adoption-primary-audit.json` authenticates the adopter and its CAS result.
  The exported source files are checked against the manifest in that verified
  result before integration. Exact commands are retained beside both audits.

The field-set refresh scanner finds zero policy deltas from `48c47c0540`.
All 567 parsed policies remain pinned; no formatting-only rewrite is adopted.
The whole wire-inventory file and fleet flag-help file pass through PB.
Final native and namespace qualification uses the commit containing this
refresh. This capture qualifies corpus bytes and grants no deployment credit.
