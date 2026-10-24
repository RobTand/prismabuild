# #2183 prerequisite recheck — not completed smoke evidence

Inspected checkout: `3f986b3ca62e79c552de227f67b2e3761a653d78`.

The #2181/W1 production prerequisite is still absent in this checkout.
`PoolQueue.sweep_orphan_passes` in `src/prismabuild/pool.py` explicitly
retains any document containing `measurement_reservation.FIELD`, without
consulting generation retirement proof. Its docstring confirms that valid,
retired and malformed election records are all retained. The existing cursor
and bounded sweep are present, but cursor advancement cannot reclaim such a
target. O2 requires an election-bearing target to survive until actual sweep
retirement. This cannot be demonstrated through that implementation without
changing the production owner, substituting a decision, or deleting the target;
none is authorized in this test/evidence package.

No production changes, smoke implementation, lifecycle execution, private-pool
observations, original CAS reads, or production census inspection were performed.
O1–O3 remain incomplete. O4's scope restriction was preserved, not evidence of a
completed lifecycle. The completed-run JSON handoff is intentionally absent:
there are no completed smoke runs to report. This is a prerequisite report,
not an alternative acceptance contract or a claim of two-host behavior.
Resume the lifecycle/evidence deliverable after the existing #2181 owner supplies
canonical retirement; do not duplicate that production work here.

## Observed verification

The following plain admitted command ran on x86/Python 3.14 and ARM/Python 3.12:

```sh
python -m pytest tests/test_measurement_election_retirement_evidence_2183.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

Each arm returned exit code 1: **6 failed, 109 passed**. All six immutable
handoff checks fail because `docs/evidence/pb2183-retirement.json` is absent;
all four mandatory guards pass. These failures accurately expose missing
acceptance evidence and are not defects in the acceptance tests. The O1/O2
smoke commands were not run because no lifecycle smoke was supplied. No check
was submitted recursively, and no install or historical source object was used.

x86 (71.32 seconds):

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"2714cd6dbc26320efabe5b27d403cac8ed335b3cad0b33f6a196ce696bb2f295","size":11456}},"operation":"d4d35d54015110d607206119dff4e98e68488bba2b7f72be6356d63a280e80a8","receipt":"198425514c12d7b4a7c97a11b1c95ab227b71e8e0414f907a5816f6aded3d79c"}]

ARM (31.83 seconds):

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"3b7eb22dee701ca617df69e7ae4c3844d54c95d870cd4594b6fccc6bce969587","size":11458}},"operation":"25743bee54c5ea75c2f8dd7f41609cfc62542298a8a735003873edade3061eb6","receipt":"8b30e95a358d98ca3cd11ccc0f64c834c18bce20d539c73bc8c644bcaf8e2e80"}]
