# Legacy record-ID classification: needrestart broker qualifier and pool exports (#1384)

`tests/test_client_boundary.py::test_legacy_record_ids_equal_the_frozen_allowlist`
failed on clean main with two IDs the scanned code spells but
`tests/boundary_allowlists/legacy_ids.txt` lacked:

```
prismaquant.prismabuild.needrestart_broker_deferral.v1
prismaquant.prismabuild.pool_export.v1
```

The two histories are different and were resolved differently.

## Genuinely new: the broker qualifier moves namespace

`tools/fleet/qualify_needrestart_broker_deferral.py` was added by #1380
(commit `f50c43561839`, 2026-09-30). It is a read-only host-local CLI; its
`schema` value is only ever printed to stdout. The tool is absent from the
observed current runtime generation (`3aff9642ab39-1790654284-1eed70850170`),
and the inspected history found no deployed reader. Saved report text in
source tests would be history, not a deployed contract; this is a statement
about what was observed and inspected, not a census of every saved copy. A
new record type uses the independent `prismabuild.*` namespace (#1250), so the
tool now reports `schema: prismabuild.needrestart_broker_deferral.v1`.
`tests/test_needrestart_broker_restart_exclusion.py` pins both the schema and
the pre-exclusion return code.

## Already deployed: the pool-export wire ID keeps its bytes

`POOL_EXPORT_SCHEMA_V1` in `src/prismabuild/pool.py` was added by
`bb57d0301245` (#1014 item 3) *after* the #1250 freeze, so the freeze snapshot
predates it; its absence from the list was a classification gap, not an
original omission. It is deployed:

- the active runtime generation `3aff9642ab39-1790654284-1eed70850170`
  carries the same string;
- `PoolQueue.record_export` refuses any other schema, and
  `PoolQueue.export_records` reads only records spelling exactly this one, so
  filed receipts and a renamed reader would stop matching. No separate
  deployed-reader compatibility promise was found in the source; this
  mechanical exact-match is the compatibility evidence;
- a reader running an older generation ignores a differently-named record, so
  changing the producer ID without a coordinated wire version is a
  compatibility break, not a guard fix.

This is a one-time compatibility correction, not a new allowance: the
constant keeps its exact bytes, the single `pool_export.v1` row is recorded
in `tests/boundary_allowlists/legacy_ids.txt` with its evidence, the
allowlist's shrink-only rule stands for every other ID, and no stored record
is migrated. Nothing here permits adding another ID later merely because it
was deployed first.

Compatibility coverage: `tests/test_paced_exports_share_a_tier.py` pins the
constant to the literal deployed ID and exercises `record_export` /
`export_records` over it; `tests/test_produced_spool_paced_export.py` covers
the paced-export producer path that files and re-reads those receipts.

## Not done

No record migration, no wire-version bump, no legacy-allowlist waiver, and no
scope change to the boundary guard. The correction adds one evidence-carrying
line for an already deployed ID and removes the other ID by moving it to its
own namespace.
