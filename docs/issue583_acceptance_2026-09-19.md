# Issue #583 acceptance record: data movement as DAG nodes (2026-09-19)

This record maps each acceptance criterion from
[issue #583](https://github.com/RobTand/prismabuild/issues/583) to the
landed implementation and the test that pins it, and names the residual
that keeps the issue open. It changes no code and no default.

## Criterion 1: a movement node with a CAS edge to its consumer

Landed. `src/prismabuild/dagster.py` carries `MovementSpec` (source tier,
destination tier, manifest-declared half-open byte range, consumer edge),
`ActionSpec` accepts one nullable `movement`, and the consumer binds the
mover's deterministic residency descriptor through an ordinary
`CASDependency` checked by the existing content-binding rule. A movement
node reserves no GPU. Pinned by
`tests/test_dagster_movement_nodes.py` (11 tests: valid graph, bad tiers
and empty ranges, no-GPU, manifest carriage, unknown consumer, missing
edge, wrong-blob binding, input prefix, same manifest, v1/v2 round-trip).

Half landed through: #590 (cluster-scoped tier admission from the
manifest), #599 (movers stage a declared range), #602 (egress node, window
publication), #609 (declared demand, pool-side fill mint), #610 (dead-mover
reclaim), #619 (adopt resident ranges), #622 (writable-plus-held supply),
#626 (inflight supply, recompute, receipt-pinned lead), #629 (one queue per
stage root), #633 (run-ahead bound, design #632), #635 (map-stale
admission, design #634), #637 (fill tokens are a rate, design #636), #639
(ARC layer 2, design #638), #641 (RAM tier, design #640). Design half #591
(closed) scoped the admission side; #589 carried the #582 opt-in stage.

## Criterion 2: compute is not admitted before its stage node reports the lead

Landed. `PoolQueue.residency_verdict` admits a consumer naming residency
leads only when every lead's `done/` record says `executed` and is still
pinned, the composed map names the certified lead, and the tier tokens are
held. Denials `residency_lead_not_resident`, `residency_map_not_composed`
and `residency_map_stale` take no tokens and age no pass. Pinned by
`tests/test_residency_is_derived_and_gates_admission.py` (13 tests,
including the bite test that patches the gate out and the stale-map
regression from action `26dfde9dd764`).

## Criterion 3: before/after on a real action

Not done. The claim for this issue is a measured delta on a real
byte-heavy action: sustained pool read rate, `held_seconds_total`, and the
consumer's sync/async read split from `zpool iostat -r`, with the consumer
reading the stage export through `PRISMABUILD_RESIDENCY_MAP`. The PrismaQuant
reader side of that contract is named in the #583 design comment and is not
delivered by PB. No delta is claimed here.

## Residual inside PB

The RAM leg of `window_pressure` is open PR #647 (reports only what the
bound would publish); incomplete RAM promotions are open PR #649.
Neither is re-attempted here. This record proposes #583 closes when #647
lands and criterion 3 is measured, or when the decider accepts the record
as the contract pin with criterion 3 tracked separately.
