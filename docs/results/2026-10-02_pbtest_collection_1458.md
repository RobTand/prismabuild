# PB #1458: native directory collection within immutable file shards

Source qualification only; not merged or deployed by this record. The runtime
that admitted all work was `d028dfee920b-1790960385-1d815cff72d1`.

The collection-policy owner was isolated first in
`344bb3ca1863329d3d3fd5fb32a0988ce6a9d46c`, after main
`b5887664d405f44d7f8f42e5cf2b9262a317c3a2`. Its unchanged behavior passed
86 neighboring tests through four PB shards before the policy fix.

The corrected RED regression, action
`c1d599db325bf075ca870ebd3b9ed647239c83131c8cee1c1198d131d7c614ed`,
reported 10 failures / 3 passes. The directory oracle passed in every one of
the four worker/shard combinations before dispatching PB's generated command.
The earlier `f21f312d` fixture used duplicate module basenames and failed its
oracle; it is retained as a confounded attempt, not causal evidence.

The integrated native matrix passed all 13 cases in action
`e01442cf2cb2e134db762bebb8fc1755db069605ca900236dfbf9cc14d26a016`.
It tests one/two pytest workers and one/two PB shards, root/nested ignore
lists/globs/custom hooks, stable parameter IDs, collection importorskip,
explicit user-file overrides, genuine import/syntax errors, ignored-only
no-widen behavior, and missing requested paths. The test intercepts only the
PB submission and runs the actual sealed child inside its already admitted
outer action; it does not launch a second dispatcher.

Final reconciliation negatives and controls passed 35 tests in
`b2527ceded1e2999c5e1936aa01e5a4c17c45ad25dd3e204b16ea2d28c2fa05e`.
Missing/inconsistent worker evidence, foreign files, duplicate ignore claims,
and a file claiming both collection and ignore all refuse. The complete-run
empty-population guard remains independently tested. The existing tmpdir
control was updated to consume the new selection argument and passed all 12
front-end tests in `83f9658439eee5299f912b85ce99066c4422948b7412cb61236cd753b8e50d09`.

The submission used the published `pbtest.py`, target interpreter
`/home/rob/venvs/pb-cpu/bin/python`, one pytest worker/native thread, two CPU
cores and 2 GiB per shard, priority -10, a 300 s action deadline, and PB-owned
file partitioning/placement. The broader front-end checks covered bounds,
thread reservations, requests, dependencies, timeout/retry/skip/trace handling,
duration packing and publication. Actual logs, node accounting and CAS claims
are indexed in `/home/rob/tmp/astra-resume-20261002/pb_collection_1458/`:
`final-selection-tests.json`, `collection-neighbors.json`,
`remaining-front-end-tests.json`, `receipt-metadata.json`,
`claim-verification.json`, and `source-and-log-audit.json`.

All accepted records have terminal exit 0, complete census responses,
matching stdout/stderr byte counts and digests, and fully hashed named CAS
payloads/claims/receipts. The read-only claim API does not independently
reverify the full worker attestation (`attestation_verified: null`); this
record does not claim that extra check. Three additional shards waited for
`measurement_census_unavailable` on DL380, then completed normally; no request
was changed or retagged to bypass admission. No GPU workload was run and no
speedup is claimed. The native matrix concerns collection correctness, not
matched application timing or a complete Tessera suite qualification.
