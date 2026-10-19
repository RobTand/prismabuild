# Completion audit at 93424

The full brief remains open. This audit fixes its source at
`93424bbc82ad5a776995208ced589c1f69d6c0e3`. Of 31 proposal cards, five have
complete source, 15 are partial, ten remain, and one awaits Rob. Of 111 original
requirements, 31 are fulfilled, 23 partial, 48 remaining, four conditional, and
five await Rob. Complete source does not establish deployment or workload proof.

The [111-row ledger](refactor_requirements_93424_2026-10-15.tsv) preserves every
original ID and wording, with engineering clarification in a separate column.
Earlier ledgers remain fixed at their own source. This checkpoint excludes all
active and isolated implementations and all changes after its pin.

Independent inspection checked 403 source citations and 22 stored successful
source, terminal, log and CAS receipt/payload chains. The combined scoped run at
`0e17e8b0ef2804b21a055f7e0d877b121138ece9` contains 6,619 passed and three
skipped unique cases across 434 complete files, with six auxiliary subtests
counted separately. All 5,230 prior combined nodes remain. Two skips require a
readable NFS mount; one is an opt-in claim-loop profile. These populations are
not a full repository suite or disjoint counts to sum.

The complete audit, hashed manifest and rerunnable standard-library source and
artifact checks are in
`/home/rob/tmp/prismabuild-refactor-mailbox-20261015/completion-audit-93424/`.
`check_artifacts.py` validates the exact original requirements, card states,
source citations, recorded populations and input hashes without project imports.
The earlier broad suite failed and remains recorded as a failure.

The audit found stale requeue text and a stale D09 summary. Later documentation
corrections do not change this frozen assessment. The remaining source work
includes claim composition, capability predicates, queue grammar and adapters,
ending/lifecycle, staged ownership, tiers, funding/batch, submission, strict
proof, residency, scratch, decomposition operations, shard verdicts, shared
fleet census, roster loading, package boundaries, wider fixtures and the
cross-generation record catalogue. The matrix identifies their exact IDs.

Five product choices remain: old adapter retirement, experimental stage
retirement, rendezvous retirement, drain profile membership, and the retained
reader window. No choice follows from silence. The 20-PR review is a disposition
recommendation; it changed no PR state.

| Card | Original title | Source state | Fulfilled | Partial | Remaining or conditional | Rob choice |
| --- | --- | --- | --- | --- | --- | --- |
| c01 | 01 · Split the claim pass: a verdict step and a reservation transaction | partial | c01.r02, c01.r03 | c01.r01 | c01.r04 | none |
| c02 | 02 · One peer-placement view for the five yield rules | complete_source | c02.r01, c02.r02, c02.r03 | none | none | none |
| c03 | 03 · Capability eligibility behind two real adapters: the offer and the live host | partial | c03.r03 | c03.r01, c03.r02 | none | none |
| c04 | 04 · Host occupancy facts: one holder census, one sample-trust rule | complete_source | c04.r01, c04.r02, c04.r03 | none | none | none |
| c05 | 05 · Queue record store: the layout and every read mode behind one module | partial | none | c05.r02, c05.r04 | c05.r01, c05.r03 | none |
| c06 | 06 · One ending model, and one way to await it | partial | none | c06.r01, c06.r03 | c06.r02 | none |
| c07 | 07 · An explicit action lifecycle with one conclude transition | remaining | none | none | c07.r01, c07.r02, c07.r03 | none |
| c08 | 08 · ProgressWatch owns the no-progress rung | complete_source | c08.r01, c08.r02, c08.r03, c08.r04 | none | none | none |
| c09 | 09 · Staged-path ownership: the vouch and the census in one module | remaining | none | none | c09.r01, c09.r02, c09.r03, c09.r04 | none |
| c10 | 10 · Tier holdings: hide the raw ledger behind domain operations | remaining | none | none | c10.r01, c10.r02, c10.r03, c10.r04 | none |
| c11 | 11 · Kept reads: trusted document versions as one src module | partial | c11.r01, c11.r02 | c11.r04 | c11.r03 | none |
| c12 | 12 · Funding: one record for both bindings, one module for its life | partial | c12.r01 | c12.r02 | c12.r03, c12.r04 | none |
| c13 | 13 · Batch ledger: one module owns a produced batch's phase, addressing and path ownership | remaining | none | none | c13.r01, c13.r02, c13.r03 | none |
| c14 | 14 · Deepen submit: one src module seals, gates and publishes a request | remaining | none | none | c14.r01, c14.r02, c14.r03, c14.r04 | none |
| c15 | 15 · Generation receipt: one stdlib-only reader for RUNTIME_VERSION.json | partial | none | c15.r01, c15.r02 | c15.r03 | none |
| c16 | 16 · Delete the superseded SLURM adapter and the Dagster adapter built on it | rob_choice | none | none | c16.r02, c16.r03 | c16.r01 |
| c17 | 17 · One role runner every role runs under | partial | c17.r02, c17.r03, c17.r04 | c17.r01 | none | none |
| c18 | 18 · Drain acknowledgement: one park-marker protocol | partial | none | c18.r01 | c18.r03, c18.r04 | c18.r02 |
| c19 | 19 · Contract registry: one home for code-capability tags | complete_source | c19.r01, c19.r02, c19.r03 | none | none | none |
| c20 | 20 · Attested core: carry only what run-local runs, and name the library face | partial | c20.r01 | c20.r02 | c20.r03 | c20.r04 |
| c21 | 21 · Residency plan: one module files, registers and decides | remaining | none | none | c21.r01, c21.r02, c21.r03, c21.r04 | none |
| c22 | 22 · Manifest read order in one src module | complete_source | c22.r01, c22.r02, c22.r03 | none | none | none |
| c23 | 23 · One stage-pool probe; retire the opt-in prewarm stage tier | partial | c23.r01, c23.r02 | none | c23.r04 | c23.r03 |
| c24 | 24 · Scratch lifetime as one module | remaining | none | none | c24.r01, c24.r02, c24.r03 | none |
| c25 | 25 · Decomposition record: the plan's files behind the module that plans | partial | c25.r01 | c25.r02, c25.r03 | c25.r04 | none |
| c26 | 26 · Shard verdict owned by pbtest_outcomes | remaining | none | none | c26.r01, c26.r02, c26.r03 | none |
| c27 | 27 · One fleet census, three renderers | remaining | none | none | c27.r01, c27.r02, c27.r03 | none |
| c28 | 28 · One fleet roster reader | partial | c28.r01 | c28.r03 | c28.r02, c28.r04 | none |
| c29 | 29 · Fleet library moves into the package; tools/fleet keeps entry points | partial | c29.r01 | c29.r03, c29.r04 | c29.r02 | none |
| c30 | 30 · Fleet world builder for tests | partial | none | c30.r01, c30.r02, c30.r03, c30.r04 | none | none |
| c31 | 31 · Record kind catalogue with a cross-generation corpus | remaining | none | none | c31.r01, c31.r02, c31.r03 | c31.r04 |
