# D38 record-only setting: recorded person decision (prismabuild#1741)

Status: decided. CEO decision dec-1010-033751-ef1d states the rule. It serves parent prismabuild#1740 criterion C1. It guides sibling prismabuild#1742 (k2). File name stays for link stability. Title states the current state.

## 1 Recorded person decision

Id: dec-1010-033751-ef1d. Date: 2026-10-10 03:39Z. Decider: CEO as Rob delegate under D60. Rob can overrule it.

Source: RobTand comment on prismabuild#1741 at 2026-10-10T04:37:43Z. Link: https://github.com/RobTand/prismabuild/issues/1741#issuecomment-6093836432. This section binds k2. Later sections mark binding or guidance.

### 1.1 Setting source

A reviewed file in the prismabuild repository holds the D38 mode. It sits next to `tools/fleet/d38_gate.py`. A change needs a reviewed pull request. No flag carries it. No environment variable carries it. No caller input reaches it.

### 1.2 Producer-readiness evidence

A record file in the same repository holds producer readiness. Its owner is the fleet owner, `pb-integrator`. The gate reads both files at the deployed main commit. The gate refuses only when the record for that invocation is absent. See section 3 for the term and the truth table.

### 1.3 Signer

The fleet owner signs. A merged pull request is the sign-off. D32 stopped seal use. Rob does not sign each change.

### 1.4 Verbatim CEO text

```text
CEO decision (D32, D60), id dec-1010-033751-ef1d, 2026-10-10 03:39Z. D38 record-only setting and producer readiness. (1) Setting source: a reviewed setting file in the prismabuild repository, next to tools/fleet/d38_gate.py. A change needs a reviewed pull request. It is not a flag or an environment variable. (2) Producer-readiness evidence: a record file in the same repository. Its owner is the fleet owner, pb-integrator. The gate reads both files at the deployed main commit. It refuses only when the record for that invocation is missing. (3) Signer: the fleet owner. The merged PR is the sign-off, because D32 turned sealing off. Rob does not sign each change. The CEO decides this as Rob's delegate under D60, and Rob can overrule it.
```

## 2 Terms

This section binds k2 as interpretation. A person confirms section 3.

- Reviewed file: a file in `RobTand/prismabuild` on `origin/main`. A reviewed pull request changed it.
- Setting file: the reviewed file from section 1.1. It holds one mode: `record` or `enforce`. No file means `record`.
- Readiness file: the record file from section 1.2. It holds one entry per invocation key. Key form: `kind:interpreter:target`. Each entry holds `ready` true or false.
- Invocation job record: per-job D38 proof. It is the preflight receipt in CAS plus the audit event. `verify_receipt` checks it. It is not the readiness file.
- Record for that invocation: the invocation job record for this job. Section 3 states this reading and its reason.
- Deployed main commit: the `origin/main` commit that built the running gate code. The gate reads the setting file and the readiness file at that commit. It uses no flag, no variable, and no caller input to find them. K2 selects the resolve method.
- Audit: an immutable event under `pb-queue/d38-audit/<job hash>/`. The gate writes it before publication. A failed write refuses.

## 3 Refuse rule and truth table

This section names a real conflict. It gives one reconciled rule. It needs a one-line person confirmation.

Conflict:

- Decision dec-1010-033751-ef1d says the gate refuses only when the record for that invocation is absent.
- Parent prismabuild#1740 criterion C1 says enforcement-on refuses only with producer evidence.
- Sibling prismabuild#1742 criterion 2 says without the evidence the gate records and does not refuse.
- If record means the readiness file, the decision contradicts the parent and k2.
- If record means the invocation job record, all three texts agree as two halves of one rule.

Reconciled rule for k2:

- The gate refuses only when all three hold. They are: setting is `enforce`, readiness entry is present and `ready` is true, and the job check fails.
- Job check fails means receipt absent or `verify_receipt` rejects it.
- In all other cases the gate publishes and writes an audit event.
- No expiry causes refusal. No digest mismatch causes refusal. D32 forbids a new seal gate.

Truth table. Inputs: setting, readiness entry, job check. Output: publish or refuse. All paths (pool, SLURM, deferred GPU) use it.

| Setting | Readiness entry for invocation | Job check | Gate result |
|---|---|---|---|
| No file (`record`) | Any | Any | Publish and audit |
| `record` | Any | Any | Publish and audit |
| `enforce` | Absent, or `ready` false | Any | Publish and audit |
| `enforce` | Present and `ready` true | Pass | Publish and audit |
| `enforce` | Present and `ready` true | Fail or absent | Refuse and audit |

Open point for the person:

- Confirm one line: record for that invocation means the invocation job record, and the table above is correct.
- If the CEO meant the readiness file, the texts conflict. Then the CEO posts a correction on prismabuild#1741 with a new id. K2 waits for it.
- Requested confirmation text: `Confirmed: dec-1010-033751-ef1d record means job receipt; refuse needs enforce plus ready plus failed job check.`

## 4 Implementation guidance for k2

This section guides k2. It does not bind k2. File names and schema are k2 choice.

- Suggested setting path: `tools/fleet/d38_settings.json` in `RobTand/prismabuild`. JSON reuses the gate reader `_json_without_duplicates`. It reuses precedent `fleet_boxes.json` and tier policy files.
- Suggested readiness path: `tools/fleet/d38_producer_readiness.json` in `RobTand/prismabuild`. Owner: `pb-integrator`.
- Suggested minimal schema `fleet.d38.producer_readiness.v1` with fields, as a list:
  - `schema`
  - `producer_id`
  - `ready`
  - `recorded_at`
  - `owner`
- Do not use `expires` as refusal input. Do not use `setting_digest` as refusal input. An expiry refusal is a stale-record refusal. A digest refusal is a new identity wall. The decision rejects stale refusal. D32 forbids new seal gates. Extra stamps may exist as info only. They never cause refusal.
- Do not use the per-job exception path `verify_exception` with `DECISION_DIR` for producer readiness. That path needs per-job CEO action. It does not scale.
- Cite symbols only, not line numbers: `ENFORCE`, `verify_receipt`, `verify_exception`, `_json_without_duplicates`, `DECISION_DIR`.

- Brief quote (short): setting file next to `d38_gate.py` needs a pull request. Readiness record is owned by `pb-integrator`; merge is sign-off; gate reads both at deployed main commit.

This section does not bind k2. It keeps traceability.

- Attempt-2 brief cites `fleetgraph#16` for a CEO design choice. That thread holds CEO-session uplift requests. A scan of 31 comments found no D38 record-only text. Decision dec-1010-033751-ef1d supersedes that label.
- Brief quote (short): setting file next to `d38_gate.py` needs a pull request; readiness record is owned by `pb-integrator`; merge is sign-off; gate reads both at deployed main commit.
- Issue prismabuild#1749 asked for concrete path, schema, and a settled refuse rule. It closed as met by dec-1010-033751-ef1d. This doc records that the refuse term still needs the one-line confirmation in section 3.

## 6 Link to k2

Sibling k2 is prismabuild#1742. It waits on this issue. Its ambiguities state that evidence form comes from k1. The person recorded dec-1010-033751-ef1d. K2 must cite that id and implement section 3.

K2 does not cite the id yet. Neither body nor comments of prismabuild#1742 hold it on 2026-10-10. The pipeline adds the id to prismabuild#1742 before prismabuild#1741 closes. Evidence for the decision is the CEO comment link in section 1.
