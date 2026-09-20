# Staged-read acceptance record — template and worked example

A scoped acceptance record is the machine-readable artifact the contract
(§10) requires per merge, launch, deploy, and completion step. One record
per step; fields differ per step (a future terminal receipt is never
demanded before launch). Reasoned exceptions cite explicit user authority;
no agent waiver exists. No performance or quality numbers are set here.

## Template (all fields required; `unknown` allowed only where noted)

```json
{
  "schema": "pb.staged_read_acceptance.v1",
  "step": "prelaunch | merge | deploy | complete",
  "contract_commit": "<contract doc commit>",
  "ledger_commit": "<ledger JSON commit>",
  "scope": ["<requirement IDs in scope>"],
  "prelaunch": {
    "submission": "<sealed request key>",
    "readiness_verdict": "<residency_verdict state per lead>",
    "tier_policy": "strict | non-staged-uncertified | scoped-user-authorization:<authority-ref>",
    "window_fit": "proven | unsupported-workset:<reason>"
  },
  "merge": {
    "branch": "<branch>",
    "issue": "<issue URL>",
    "validated_repairs": [{"id": "<REQ-ID>", "actions": ["<action_key>"], "outcome": "<terminal status + receipt>"}],
    "remaining_gaps": ["<REQ-ID + reason>"]
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

Only the object matching `step` needs full contents; the other three
steps read `{"status": "not-this-step"}`. `unknown` is legitimate for
`role_convergence` and `open_unknowns` entries — never for hiding a
required check.

## Worked example: PB720 merge step (real keys)

```json
{
  "schema": "pb.staged_read_acceptance.v1",
  "step": "merge",
  "contract_commit": "not-applicable (predates contract)",
  "ledger_commit": "not-applicable (predates ledger)",
  "scope": ["SAFE-03"],
  "prelaunch": {"status": "not-this-step"},
  "merge": {
    "branch": "fix/pbmcp-power-reporting-20260920",
    "issue": "https://github.com/RobTand/prismabuild/issues/719",
    "validated_repairs": [
      {"id": "SAFE-03", "actions": [
        "0f54fe1348fe6f2ce775abe811c7cd1077d6874bc72ee749f7d23bc4e18e9a4a",
        "263957bf71ea8d2bb6a97e898d30e6f1675866498790f6dee8976a16f05cf28d",
        "a62a13a366ca288bfb264b9e9fdf6c2ba9dfa28675094cc1c5ea876de15cb8e1",
        "0b5d2fa45ed81204cf718d173951d2119e0d48a03de3c4ad57c999ff4b7159f7"],
       "outcome": "all done dl380g10/sparky rc 0; receipts filed; 183 passed 3/3 shards"}
    ],
    "remaining_gaps": ["staged-reader strictness: out of scope for this repair"]
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
