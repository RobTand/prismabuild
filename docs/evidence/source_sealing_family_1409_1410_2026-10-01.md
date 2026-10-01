# Source-sealing family census — PB1409 / PB1410

Owner: Sol session row-startup. Bundle inventory is **in progress**, not a closure or performance claim.

## Current population and acceptance

| Site / seam | Status | Remaining acceptance |
| --- | --- | --- |
| `src/prismabuild/core.py::git_checkout_identity` and its canonical Git runner | Existing commit-plus-working-tree identity; 30s fail-closed timeout. Baseline timeout contract test passed. | Inventory every identity caller; preserve tracked/untracked bytes, special-inode refusal, configuration isolation, plain-directory semantics and worker/submitter equality. No cached-stat shortcut or invented fallback. |
| `tools/fleet/pbrun.py::_snapshot_git` | Existing 120s fail-closed Git runner. Baseline timeout contract test passed. | Shared source-seal context/refusal coverage across the actual snapshot phase family; no silent retry/deadline increase. |
| `tools/fleet/pbrun.py::_seed_index_roster` / `build_git_checkout_snapshot` | PB1409 historical timeout during forced source-roster staging. Cause uncertain. | Inspect complete roster/index/size/transformation/link/history/CAS phases; genuine RED for chosen implementation. Preserve source bytes, size/type checks, source identity before/after sealing and deterministic bundle output. |
| Source-seal consumers in submission/template/release paths | Population not yet exhausted. | Inventory shared abstraction callers and classify CPU-safe consolidation versus deliberately distinct semantics; do not turn regex leads into findings. |
| Campaign ZFS tracked-file read stalls (PB1410) | Historical blocked reads observed; partial private rematerialization did not prove recovery. Pool health alone does not prove recovery. | Storage-owner diagnosis may be a separate hold. Do not claim that source refusal tests repair the dataset or waive identity. Name exact input/evidence needed before any parent closure. |

## Evidence mapping

Baseline tests-only head `1bbc502d11066227dc359c27a1f1abcd9a74d920` on base `c49658332c4e319dcaec02db30e7029d68d9f956`: PB `2a8ed6e9dcfcd2ae0af3ab6f69a3772d3bbcf2e8e0056df2cda815ffc99210f8`, 19 collected/ran/passed, no skips. Terminal/log/claim/payload/snapshot parent verified; worker attestation not checked. Durable receipt/source mapping: `/home/rob/tmp/claude-campaign-20260926/tmp/row-startup/pb1409-1410-contracts-inspected.json`.

This is baseline functional coverage, **not** a behavioral RED, final bundled-head qualification or root repair. Final delivery requires genuine RED witnesses for behavior changes, one combined targeted regression/ratchet/compile PB gate, one independent exact-head Sol review and queue-owned integration. Related focused commits belong in one coherent PR. Use separate `Closes` references only for fully satisfied issues; partial parents receive `Refs` with their remaining acceptance. No timing/throughput claim without before/after in-process profiles and both-box Netdata evidence. No new measurement window is activated by this census.
