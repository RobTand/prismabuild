# Reconciled PrismaBuild card summary

Current source `986cedeffae837318210cbdbd8d51351a418b7c3`. Corrected historical audit `36cebfbcc1899496733ab33404242a99c2c30d67`. Original proposal baseline `6073c9d36555d92cdf67ee58ebc7d46bc9d80512`.

All 31 cards and 111 original requirement IDs are retained. The actual Claude review supplied 25 findings, all incorporated after pinned-source inspection.

| Revision | Complete | Partial | Unfinished | Rob choice |
| --- | ---: | ---: | ---: | ---: |
| Old published ledger | 0 | 9 | 21 | 1 |
| Corrected old ledger | 0 | 8 | 22 | 1 |
| Current pinned source | 1 | 7 | 22 | 1 |

The four source-state advances are c22.r02, c22.r03, c23.r01 and c23.r02. Card 22 now completes its original in-process ownership and result-preservation scope. B01 also advances c29.r03 within its existing partial state. B02 is a separate citation repair. B04 is a separate qualified generation-overwrite fix; the original lifecycle redesign remains unfinished.

Tests and result-equivalence evidence are bounded qualifications. Runtime publication, deployment, workload proof and a final suite over every combined change are unclaimed. B05/B06 and B07 are in progress outside this pin. F1 has 31 candidate passes outside this pin and earns no integrated credit.

| Card | Corrected old | Current | Fulfilled original requirements | Scope and backlog references |
| --- | --- | --- | --- | --- |
| c01 Claim pass: verdict step and reservation transaction | unfinished | unfinished | none | c01.r01, c01.r02, c01.r03, c01.r04, B03 |
| c02 Peer-placement view | unfinished | unfinished | none | c02.r01, c02.r02, c02.r03, B08a, B08b |
| c03 Capability eligibility with offer and live-host adapters | unfinished | unfinished | none | c03.r01, c03.r02, c03.r03 |
| c04 Host occupancy census and sample trust | unfinished | unfinished | none | c04.r01, c04.r02, c04.r03 |
| c05 Queue record store and three-state reads | unfinished | unfinished | none | c05.r01, c05.r02, c05.r03, c05.r04 |
| c06 Ending model and await module | unfinished | unfinished | none; preexisting ingredient c06.r01 | c06.r01 broader model, c06.r02, c06.r03 |
| c07 Action lifecycle and conclude transition | unfinished | unfinished | none | c07.r01, c07.r02, c07.r03, B03, B04, B16, F1 |
| c08 ProgressWatch owns the no-progress rung | partial | partial | c08.r03; partial c08.r02 | c08.r01, c08.r04 |
| c09 Staged-path ownership | unfinished | unfinished | none | c09.r01, c09.r02, c09.r03, c09.r04 |
| c10 Tier holdings domain operations | unfinished | unfinished | none | c10.r01, c10.r02, c10.r03, c10.r04 |
| c11 Kept reads and filesystem trust | partial | partial | c11.r01, c11.r02; partial c11.r04 | c11.r03 |
| c12 Funding record and funding lifecycle | partial | partial | c12.r01; partial c12.r02 | c12.r03, c12.r04, B08c |
| c13 Produced-batch ledger | unfinished | unfinished | none | c13.r01, c13.r02, c13.r03, B03 |
| c14 Submission seals, gates and publication | unfinished | unfinished | none | c14.r01, c14.r02, c14.r03, c14.r04 |
| c15 Stdlib generation-receipt reader | unfinished | unfinished | none | c15.r01, c15.r02, c15.r03, B09 |
| c16 Retire earlier SLURM and Dagster adapters | rob_choice | rob_choice | none | c16.r01, c16.r02, c16.r03, B13 |
| c17 Role runner and completion boundaries | unfinished | unfinished | none | c17.r01, c17.r02, c17.r03, c17.r04, B05, B06, B09 |
| c18 Park-marker protocol and drain proof | unfinished | unfinished | none | c18.r01, c18.r02, c18.r03, c18.r04, B09 |
| c19 Pure code-capability contract registry | unfinished | unfinished | none | c19.r01, c19.r02, c19.r03, B07 |
| c20 Attested core library face | unfinished | unfinished | none; preexisting ingredient c20.r01 | c20.r02, c20.r03, c20.r04, B02, B13, B15 |
| c21 Residency filing, registry and window decision | unfinished | unfinished | none | c21.r01, c21.r02, c21.r03, c21.r04 |
| c22 Manifest read-order owner | partial | complete | c22.r01, c22.r02, c22.r03 | U1 |
| c23 Stage-pool probes, source marks and optional stage retirement | partial | partial | c23.r01, c23.r02 | c23.r03, c23.r04, B01, B14 |
| c24 Scratch lifetime owner | unfinished | unfinished | none | c24.r01, c24.r02, c24.r03 |
| c25 Decomposition record owner | partial | partial | c25.r01; partial c25.r02, c25.r03 | c25.r04 |
| c26 Shared shard verdict | unfinished | unfinished | none | c26.r01, c26.r02, c26.r03 |
| c27 Fleet census with separately bounded sections | unfinished | unfinished | none | c27.r01, c27.r02, c27.r03 |
| c28 Fleet roster reader | partial | partial | c28.r01; partial c28.r03 | c28.r02, c28.r04, B08d |
| c29 Fleet library in the package | partial | partial | c29.r01; partial c29.r03, c29.r04 | c29.r02, B01 |
| c30 Fleet-world test builder | unfinished | unfinished | none | c30.r01, c30.r02, c30.r03, c30.r04, B10 |
| c31 Record-kind catalogue and cross-generation corpus | unfinished | unfinished | none | c31.r01, c31.r02, c31.r03, c31.r04, B11 |

Five product choices remain unanswered: adapter retirement, experimental --stage retirement, qualification rendezvous retirement, drain-profile membership including one-shot workers in resign, and the supported retained-reader window. No defaults are assumed.

The compact requirement index is [refactor_requirements_2026-10-15.tsv](refactor_requirements_2026-10-15.tsv). Its corrected_baseline_state column describes the corrected 36ceb audit, not source fulfillment at the original 6073 baseline. Source references are portable `revision:path:line` citations.

The full review artifact and reproducible generator with frozen inputs are published in `/home/rob/mailbox/claude-codex-prismabuild-refactor/attachments/`. Regenerate the unpacked bundle with `python3 build_reconciled.py --repo /path/to/checkout --output /path/to/review-artifacts`. This generator reads source through Git and does not execute project code. See [the resolution record](refactor_resolution_2026-10-15.md) for evidence beyond this source pin.
