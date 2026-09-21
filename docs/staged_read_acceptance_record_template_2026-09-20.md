# Staged-read acceptance record — template and worked example

A scoped acceptance record is the machine-readable artifact the contract
(§11) requires per merge, launch, deploy, and completion step. One record
per step; fields differ per step (a future terminal receipt is never
demanded before launch). Reasoned exceptions cite explicit user authority;
no agent waiver exists. No performance or quality numbers are set here.

A merge record is not a deployment record. A source merge, a staged runtime
generation, and an activated generation are three distinct facts: record
`deploy: pending` until activation is observed, and never cite a staged
generation as deployed support (the PB783 case in the dated addendum is the
worked example). Where a claim depends on persistence, state the observed
storage policy and keep it separate from the producer commit boundary;
records and receipts do not prove power-loss durability (contract DUR-01).

## Template (all fields required; `unknown` allowed only where noted)

```json
{
  "schema": "pb.staged_read_acceptance.v1",
  "step": "prelaunch | merge | deploy | complete",
  "contract_commit": "<contract doc commit>",
  "ledger_commit": "<ledger JSON commit>",
  "scope": ["<requirement IDs in scope>"],
  "prelaunch": {
    "submission": {
      "sealed_key": "<optional until submit; recorded when submitted>",
      "declares": ["inputs", "ranges", "progress", "runtime"]
    },
    "readiness_gate": "PB-owned verdict required at claim (residency_verdict per lead); declared here and evaluated by the claiming worker — never a human-obtained ready verdict, and no sealed key is required before submission",
    "tier_policy": "strict | scoped-user-authorization:<authority-ref>",
    "window_fit": "proven | unsupported-workset:<reason>"
  },
  "merge": {
    "branch": "<branch>",
    "issue": "<issue URL>",
    "validated_repairs": [{"id": "<REQ-ID>", "actions": ["<action_key>"], "outcome": "<terminal status + receipt>"}],
    "remaining_gaps": ["<REQ-ID + reason>"],
    "note": "<optional: what this record does NOT claim>"
  },
  "deploy": {
    "generation": "<runtime generation id>",
    "role_convergence": "observed | unknown",
    "strict_reader_recheck": "pass | fail:<reason> | unknown"
  },
  "complete": {
    "application_gates": ["<gate: verdict + output path>"],
    "open_unknowns": ["<unknown + why unremarkable or next measurement>"]
  },
  "exceptions": [{"requirement": "<REQ-ID>", "authority": "<explicit user authority ref>", "scope": "<ranges/steps>"}]
}
```

`tier_policy` has two alternatives only: `strict`, or
`scoped-user-authorization:<authority-ref>` naming the ranges, the reason,
and the explicit user authority on the sealed request. There is no
standalone non-staged alternative: uncertified status never waives a
forbidden tier.

Only the object matching `step` needs full contents; the other three
steps read `{"status": "not-this-step"}`. `unknown` is legitimate for
`role_convergence` and `open_unknowns` entries — never for hiding a
required check.

A documentation review record (schema/ID/enum coherence of a docs-only
merge, as in the worked example) is an allowed acceptance record: it
proves documentation coherence and nothing runtime. It must state that
scope explicitly and never borrow runtime proof.

## Worked example: documentation-contract merge step (this lane)

Documentation coherence only. It proves no invariant, no SAFE-03, and no
test verdict — pass counts from other lanes' suites are cited nowhere
here. (PB720 keys live in the delivery record and the VER-01 ledger
entry, not in this example.)

```json
{
  "schema": "pb.staged_read_acceptance.v1",
  "step": "merge",
  "contract_commit": "<HEAD of docs/formal-staged-read-contract-20260920 at review>",
  "ledger_commit": "<HEAD of docs/formal-staged-read-contract-20260920 at review>",
  "scope": ["SC-03"],
  "prelaunch": {"status": "not-this-step"},
  "merge": {
    "branch": "docs/formal-staged-read-contract-20260920",
    "issue": "https://github.com/RobTand/prismabuild/issues/723",
    "validated_repairs": [],
    "remaining_gaps": ["all proposed enforcements: out of scope for a docs lane; see ledger axes"],
    "note": "Checks performed: ledger JSON parses (56 requirements at this merge; 69 after the 2026-09-21 produced-output update, v3 schema); every ledger ID appears in the doc and vice versa; zero stale trust-mode enums across doc, ledger, and template. Claims nothing beyond documentation coherence."
  },
  "deploy": {"status": "not-this-step"},
  "complete": {"status": "not-this-step"},
  "exceptions": []
}
```

## 722 note (read, not re-verified in this lane)

Root accepted the shared-path scope on 74 plus prior 172 receipts (main
`0467e9e`, PR #722). That acceptance covers the shared-path worker's lane
only: still not deployed-evidence and still not read-lease proof for this
contract's SM-02/SM-03/INV-07 axes, which stay `proposed`/`unknown` until
their own acceptance records exist. No behavioral test is rerun for docs;
this lane merges main for reference drift only.
