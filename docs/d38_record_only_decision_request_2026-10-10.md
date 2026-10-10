# D38 record-only setting: draft for person approval (prismabuild#1741)

Status: draft. This note is not a decision. A person must record the decision. It serves parent prismabuild#1740 criterion C1. It unblocks sibling prismabuild#1742 (k2).

## Source brief

The attempt-2 brief states a CEO design choice. The brief labels it `fleetgraph#16`. Quote:

```text
Setting file in the prismabuild repository (for example a reviewed TOML or JSON file next to tools/fleet/d38_gate.py). A change needs a pull request. Producer readiness is a signed record file in the same repository, owned by the fleet owner (pb-integrator). The gate reads both files at the reviewed main commit and refuses when the record is missing, stale, or not signed. The fleet owner signs. CEO notes: without a signature ceremony. D32 turned sealing off, so a merged pull request is the approval. Put the setting file next to tools/fleet/d38_gate.py, with a change needing a reviewed PR. Producer readiness is a record file in the same repository that pb-integrator owns, and its merge is the sign-off. The gate reads both at the deployed main commit. It refuses only when the record for that invocation is missing. Rob does not sign each change.
```

Decider: CEO. Date: 2026-10-10 (brief date). Stated id: `fleetgraph#16`.

## Check of the stated id

The GitHub thread `RobTand/fleetgraph#16` holds CEO-session uplift requests. A scan of all 31 comments on 2026-10-10 found no D38 record-only text. It mentions the D38 dry run once, in the measurement route. So the D38 text above is verifiable only from the brief, not from that thread. The person must confirm the canonical id. Sibling k2 must cite that id.

## Setting source: options

The setting source must be a reviewed file in a repository. No flag or variable may carry it. No caller input may reach it. Candidates:

- A: `tools/fleet/d38_settings.json` in `RobTand/prismabuild`, changed only by reviewed pull request. JSON matches `tools/fleet/fleet_boxes.json`, `tools/fleet/local_tier_policy.json`, and `tools/fleet/ram_tier_policy.json`. The gate already parses strict JSON (`tools/fleet/d38_gate.py`, `_json_without_duplicates`). No new parser enters the gate.
- B: `tools/fleet/d38_settings.toml` in `RobTand/prismabuild`, changed only by reviewed pull request. TOML allows comments. It adds a parser beside the gate JSON path.

Recommendation: option A. It reuses the local JSON precedent and the gate JSON reader. It keeps one parse path for review.

## Producer-readiness artifact: options

The artifact must name its owner. The gate must check it at the deployed main commit. Candidates:

- A: `tools/fleet/d38_producer_readiness.json` in `RobTand/prismabuild`, owned by `pb-integrator`. Schema `fleet.d38.producer_readiness.v1` with fields: `schema`, `setting_digest`, `producer_id`, `ready`, `recorded_at`, `expires`, `owner`. The digest binds the record to the exact setting file. Merge of a reviewed pull request is the sign-off. The brief states that no signature ceremony exists and cites D32.
- B: a per-job CEO decision file under the existing exception path (`DECISION_DIR`, `tools/fleet/d38_gate.py:52`, `verify_exception`). This is the status quo ante. It needs Rob or CEO action per job. It does not scale to producer qualification.

Recommendation: option A. It matches the parent goal ("require fleet-owner producer readiness before activation"). It matches the brief ("pb-integrator owns the record, merge is the sign-off").

## Gate check

The gate reads both files at the deployed main commit. Rules for sibling k2:

- With no setting file, pool, SLURM, and deferred-GPU submissions publish. The gate records each check as an audit event.
- With enforcement on, a fresh bound readiness record, and a failed check, the gate refuses.
- With enforcement on and no fresh bound record, the gate records and does not refuse.
- No flag, variable, or caller input changes the setting. The `ENFORCE` docstring (`tools/fleet/d38_gate.py:54-59`) and `docs/design.md` state the new contract.

Fresh means `recorded_at` is past and `expires` is future. Bound means `setting_digest` equals the digest of the deployed setting file.

## Signer

The fleet owner signs. Owner `pb-integrator` owns the readiness file. Merge of its reviewed pull request is the sign-off. Rob signs nothing per change. The person must confirm this split. The issue lists it as the open ambiguity.

## Open point for the person

The brief holds two refuse rules. One says the gate refuses when the record is missing, stale, or not signed. The other says it refuses only when the record for that invocation is missing. Sibling k2 says the gate records and does not refuse without evidence. These conflict. Recommendation: adopt the k2 rule. The person must settle it.

## Link to k2

Sibling k2 is prismabuild#1742. It waits on this issue (`issuegraph:after ref=prismabuild#1741`). Its ambiguities state that the exact form of the evidence comes from k1. Once the person records the decision with an id, k2 cites that id and implements the gate change.
