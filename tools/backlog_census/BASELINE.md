# Reconcile the October 25 planning snapshot

The original scripts reproduce 443 in-scope tickets in 216 work units. The new
rules preserve all 443 tickets and produce 220 work units from the same input.
This is an offline comparison, not a current GitHub census.

| Repository | Tickets | Original units | Corrected units |
|---|---:|---:|---:|
| RobTand/prismaquant | 180 | 77 | 79 |
| RobTand/prismabuild | 102 | 67 | 69 |
| RobTand/tessera | 161 | 72 | 72 |

Thirteen tickets change roots. Five additional roots come from accepting only
the task's named text-parent lines. The cross-repository correction moves one
ticket into an existing Tessera work unit and removes its former standalone
PrismaQuant unit.

| Ticket | Original root | Corrected root | Explanation |
|---|---|---|---|
| prismabuild#2039 | #1895 | #2039 | `Split from #1895` is outside the named parent phrases. |
| prismabuild#2040 | #1661 | #2040 | `Split from #1661` is outside the named parent phrases. |
| prismabuild#2088 | #1661 | #2040 | Native parent #2040 remains; the old split link above it is excluded. |
| prismabuild#2089 | #1661 | #2040 | Native parent #2040 remains; the old split link above it is excluded. |
| prismaquant#1303 | #1295 | #1303 | `Part of #1295` appears inside a prose paragraph, not a parent line. |
| prismaquant#2430 | #2411 | #2430 | `Parent: #2411` is outside the named parent phrases. |
| prismaquant#2458 | #2411 | #2430 | Its `Part of` line remains; the `Parent:` link above it is excluded. |
| prismaquant#2487 | #2427 | #2487 | `Parent: #2427` is outside the named parent phrases. |
| prismaquant#2566 | #2566 | tessera#702 | Follow `Part of RobTand/tessera#1150` and that issue's ancestry. The old script ignored cross-repository text links. |
| prismaquant#2894 | #2411 | #2430 | Native parent #2458 reaches #2430; the `Parent:` link above it is excluded. |
| prismaquant#2911 | #2411 | #2430 | Native parent #2430 remains; the `Parent:` link above it is excluded. |
| prismaquant#2912 | #2411 | #2430 | Native parent #2430 remains; the `Parent:` link above it is excluded. |
| prismaquant#2913 | #2411 | #2430 | Native parent #2430 remains; the `Parent:` link above it is excluded. |

There are no added or removed in-scope tickets. Priority counts and title
fallback counts match the planning report. Each input file is retained in the
original planning workspace at
`/home/rob/.paperclip/instances/default/workspaces/12fb83a6-a28c-4820-9b89-777ff089369f/census/raw/`.

| File | SHA-256 |
|---|---|
| prismaquant.json | c118f62ac526151aabb6ef15127b9f8ada8b35e33ee12baafcc80d23c30fecab |
| prismabuild.json | 4376aedbf347db3b2ee9ce5f3fee3f04ffbc945fd0df6522332ec3730e6fc2a8 |
| tessera.json | 07715601783bb3b11c046e6cc43a499c4bea8d6aaf71276fbafc0a7a0935cc7d |

Fleet action `4858e80bb6abef0d57279dce59c103c2e77b6c0f9400a91fd7cab622677c2557`
ran the unchanged original scripts against those files, asserted the complete
saved report, then ran the new graph and reports. Its verified CAS receipt is
`c50477752af61bbf9db1358cf76c2c29877ecf5621387cb5d4097bcb378610aa`.

The open-only planning data cannot prove dead blockers, current default-branch
fixes, source-reviewed Hindsight findings, or historical unit flow. Those remain
live-collection work. No GitHub issue changes are authorized by this comparison.
