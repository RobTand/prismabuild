# Core current-writer golden refresh for #1909

The #1888 Core change altered its attested source identity. The namespace gate
at `run-w3jqu3hu` consequently reported six current-writer byte mismatches.
Only `core-receipt`, `core-attestation`, and `core-runtime`, each in the
in-process and script variants, carry the superseded Core digest.
`core-claim` and the other current goldens do not carry it.

The current Core source SHA-256 changes from
`eb094d4ec65a20c4c2de86aaa086ab3e87e5c05964e7fd86474c783550136659`
to `4fc1a4eb669c7707f5be7b49293a0faaeba4d6c29a2a450932cb878e228de3fd`.
The replacement wire bytes change only that source size and digest and the
dependent runtime, attestation, and receipt digests. No wire shape changes.

## Capture and adoption

Evidence is retained under
`/mnt/shared/pbtl-probe-tmp/evidence/1909-goldens/home-pbt17/`.
The frozen capture source is `f4960e6af1433ea71289c808fa17a9bc730dc243`.
Published `pbrun.py` admitted capture and adoption with one CPU, one GiB,
priority -10, native threads one, and `TMPDIR=/home/rob/pbt17`.
This scratch path follows the lead's corrected instruction and the corpus
procedure. Earlier `/var/tmp/pbt17` runs remain excluded in the parent evidence
directory's `excluded-actions.json` and supply no acceptance credit.

- Reproduction action `d2e647e4a65f69381ff5550c11a0e16bf6feab2d4977af46da2e390bccced5ae`
  fails the isolated `core-runtime/in-process` current-writer comparison.
- Capture action `0aa433fa67a642c6a8ddbeb8004191c30304a435d8beabec4c0ca0142dda0871`
  independently reproduces all six current and twelve retained historical
  writer records and passes both consumer directions.
- Adoption action `f9f2c11b00a6b55d630b3716edd197b851ace29ad8d3f4ab75bac32dc4883b6d`
  runs `refresh_compat_runtime.py`, which invokes `adopt_compat_capture.py`
  with the verified primary audit. It preserves all twelve historical rows
  byte-for-byte and archives the complete superseded current files under
  `tests/compat/history-controls/superseded-runtime-c8e6cab9ffb7b596/`.

`capture-primary-audit.json` and `adopter-primary-audit.json` verify the request,
source Git objects, input blobs, logs, runtime, CAS receipt, result, and cleanup.
The exported adopter-owned files are hash-checked before source integration.

## Source reviews and limits

Core's immutable-helper pin remains correct. The source-only scanner's complete
field-set output equals the committed field inventory. No required test enforces
the old Core source-review dispositions against the live repository. Those
reviews therefore remain unchanged under the task's test-required-only scope.
The broader `wire_inventory.py --require-complete` scan is already incomplete,
including stale reviews for Core, PoolQueue, pbmcp, and tier_cycle. Its retained
`inventory-before` report grants no complete-inventory acceptance.

The exact namespace rerun command and supplemental x86 command, shard results,
and final primary audit are retained with the capture evidence. This refresh
qualifies corpus bytes only. It grants no deployment or whole-campaign approval.
