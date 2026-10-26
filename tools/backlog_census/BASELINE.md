# Reconcile the October 25 planning snapshot

The original scripts reproduce 443 in-scope tickets in 216 work units. The
corrected parser preserves all 443 tickets and produces 218 work units from
the same input. This is an offline comparison, not a current GitHub census.

| Repository | Tickets | Original units | Corrected units |
|---|---:|---:|---:|
| RobTand/prismaquant | 180 | 77 | 77 |
| RobTand/prismabuild | 102 | 67 | 69 |
| RobTand/tessera | 161 | 72 | 72 |

Six tickets change roots. Excluding split declarations and an inline prose
reference adds three roots. Following a cross-repository parent removes one
standalone root. No in-scope ticket enters or leaves the planning population.

| Ticket | Original root | Corrected root | Explanation |
|---|---|---|---|
| prismabuild#2039 | #1895 | #2039 | `Split from #1895` describes origin, not parentage. |
| prismabuild#2040 | #1661 | #2040 | `Split from #1661` describes origin, not parentage. |
| prismabuild#2088 | #1661 | #2040 | Native parent #2040 remains; the split reference above it is excluded. |
| prismabuild#2089 | #1661 | #2040 | Native parent #2040 remains; the split reference above it is excluded. |
| prismaquant#1303 | #1295 | #1303 | `Part of #1295` appears inside a prose paragraph, not a parent line. |
| prismaquant#2566 | #2566 | tessera#702 | Follow `Part of RobTand/tessera#1150` and that issue's ancestry. The original parser ignored cross-repository text links. |

Review round 1 found that the initial implementation incorrectly omitted
`Parent:` declarations and reported 220 units. Restoring that declaration
keeps prismaquant#2430, #2458, #2894 and #2911–#2913 under D50 #2411, and
#2487 under #2427. Those seven root changes were defects in the initial
implementation, not intentional corrections to the planning census. They no
longer appear in the table. Regression fixtures cover `Parent:` and keep
`Split from` excluded.

The initial claim heuristic also mistook the ticket template's `Done when:`
for a completion claim. The corrected planning replay has zero claim candidates
and two duplicate clusters. Comments were absent from the planning input, so
these counts do not establish the live candidate population. Duplicate clusters
are connected components of matching titles, with no close inferred from
similarity.

Priority counts and title fallback counts match the planning report. The original
input files were collected in the planning workspace at
`/home/rob/.paperclip/instances/default/workspaces/12fb83a6-a28c-4820-9b89-777ff089369f/census/raw/`.

| File | SHA-256 |
|---|---|
| prismaquant.json | c118f62ac526151aabb6ef15127b9f8ada8b35e33ee12baafcc80d23c30fecab |
| prismabuild.json | 4376aedbf347db3b2ee9ce5f3fee3f04ffbc945fd0df6522332ec3730e6fc2a8 |
| tessera.json | 07715601783bb3b11c046e6cc43a499c4bea8d6aaf71276fbafc0a7a0935cc7d |

The planning workspace is mutable. Use the copies archived with PRI-2 and check
the hashes above before a replay. The first archive preserves the original
bytes. The review archive differs only in prismabuild#2243 `updatedAt`, from
`2026-10-24T11:14:18Z` to `2026-10-25T19:27:48Z`. Its prismabuild input hash is
`be0303925cb6d9efdc70f72f14203dc3fd082005f97f3dcef1ebbe3d30c7f1e8`.
All membership, priority and relationship fields are identical. This timestamp
change does not alter the ticket or work-unit reconciliation.

Fleet action `da7bfe57cee58543654bc45a237a7912376e13ccce591f3462de87a17baa8b21`
passed all 61 tests without skips, replayed the unchanged original scripts
against that review copy, and ran the corrected graph and reports. Its
content-verified CAS receipt is
`0f4995f71986187a3261526b14ac3bd3484b38384106767fec07aa00fdb7b54d`.
Terminal success, immutable log hashes, scope cleanup and the result payload
were checked.

Live additions, closures, priority changes and relationship edits must be
reconciled separately against a successful live collection. The open-only
planning input cannot prove dead blockers, default-branch fixes or historical
unit flow. No GitHub issue changes are authorized by this comparison.
