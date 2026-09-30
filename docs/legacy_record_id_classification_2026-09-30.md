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
`schema` value is only ever printed to stdout. The observed fleet manifest
does not carry this tool in any published generation, so the ID has no
deployed producer or reader: the tool, its test, and the pre-deployment run
`07cc1bae` on dl380g10 (which correctly reported the broker not deferred
before the configuration-only rollout) are its whole history. Source tests
may have saved the old report text; that is history, not a deployed wire
contract. A new record type uses the independent `prismabuild.*` namespace
(#1250), so it now reports
`schema: prismabuild.needrestart_broker_deferral.v1`.
`tests/test_needrestart_broker_restart_exclusion.py` pins that namespace.

## Already deployed: the pool-export wire ID is retained verbatim

`POOL_EXPORT_SCHEMA_V1` in `src/prismabuild/pool.py` was added by
`bb57d0301245` (#1014 item 3) *after* the #1250 freeze, so the freeze snapshot
simply predates it — its absence from the list was a classification gap, not
an original omission. It is deployed, and deployment makes it legacy:

- the active runtime generation `3aff9642ab39-1790654284-1eed70850170`
  carries the same string;
- `PoolQueue.record_export` refuses any other schema, and
  `PoolQueue.export_records` reads only records spelling exactly this one, so
  filed receipts and a renamed reader would stop matching. No separate
  deployed-reader compatibility promise was found in the source; this
  mechanical exact-match is the compatibility evidence;
- a reader running the old generation ignores a differently-named record, so
  changing the producer ID without a coordinated wire version is a
  compatibility break, not a guard fix.

The constant therefore keeps its exact bytes. It is classified as a legacy ID
in `tests/boundary_allowlists/legacy_ids.txt` next to its deployment evidence —
the correction the issue asks for — rather than renamed to make the guard
pass. No stored record is migrated, and the guard itself is unchanged.

Compatibility coverage: `tests/test_paced_exports_share_a_tier.py` pins the
constant to the literal deployed ID and exercises `record_export` /
`export_records` over it; `tests/test_produced_spool_paced_export.py` covers
the paced-export producer path that files and re-reads those receipts.

## Not done

No record migration, no wire-version bump, no legacy-allowlist waiver, and no
scope change to the boundary guard. The correction adds one evidence-carrying
line to the allowlist and removes the other ID by moving it to its own
namespace.
