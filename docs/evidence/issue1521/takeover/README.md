# Attested-core repair evidence

The unchanged finite-number predicate moved from `core.py` to
`numeric_fields.py`. Core matches its pre-regression bytes. Existing numeric
assertions, exact goldens, source capture, source pins and the field inventory
are preserved. Integrated head `85fa57bb7194afe9562193a7368d2e3a403a9293`
retains published head `a7b0b8def49fa992e7676304753eced414b29b78` as a parent.

| Qualified source | PB actions | Passing native executions | Source bundles |
| --- | ---: | ---: | ---: |
| `affe5b27eedd8e2ac55e9b07e52ad7b64e0e1db8` | 28 | 480 | 12 |
| `85fa57bb7194afe9562193a7368d2e3a403a9293` | 19 | 553 | 2 |

Both selections have zero skips. The numeric table ran five times per
architecture, including two xdist runs per architecture. The integrated
selection covers 24 files and all six original golden IDs. The two action
populations are disjoint. Their 1,033 executions include repeated tests.

`validation.json` records the scope and counts. The two audit files bind each
action to its source, request, native outcomes, complete logs, canonical CAS
receipt and payload, and empty, settled cleanup. Controller report hashes were
also checked. `core-adjudication.json` preserves the original GREEN/RED boundary
evidence. Historical evidence retains its source identity.

The numeric, adjacent and integrated JSON inputs below are derived action/exit
indices. `report-sources.json` records the original full report paths and hashes.
The original reports and full immutable streams stay in shared evidence storage.

The following commands inspect records and Git objects without submitting or
executing tests. Run from the task worktree on a coordinator with shared CAS
access. Store output outside the checkout.

```bash
evidence_dir=docs/evidence/issue1521/takeover
audit_dir=/mnt/shared/paperclip-runs/PRI-47/review-readback
mkdir -p "$audit_dir"
python3 "$evidence_dir/audit_qualification.py" \
  --checkout "$PWD" \
  --head affe5b27eedd8e2ac55e9b07e52ad7b64e0e1db8 \
  --out "$audit_dir/repair.json" \
  "$evidence_dir"/numeric-x86-[0-4].json \
  "$evidence_dir"/numeric-aarch64-[0-4].json \
  "$evidence_dir/adjacent.json"
python3 "$evidence_dir/audit_qualification.py" \
  --checkout "$PWD" \
  --head 85fa57bb7194afe9562193a7368d2e3a403a9293 \
  --out "$audit_dir/integrated.json" \
  "$evidence_dir/integrated.json"
python3 "$evidence_dir/audit_reconciliation.py" \
  --repo "$PWD" --out "$audit_dir/source.json"
```

For the original boundary readback, copy `audit_core_probe.py`, `core-plan.json`,
`core-main-proof.json` and both `core-*.json.progress.json` files into an evidence
directory outside the checkout, then run the copied script there. It reads the
original action keys without submission. The retained main proof remains
historical evidence rather than a new main run.

`merge-acceptance.json` is scoped source qualification. Engineer23's fresh
independent Standards and Spec review and PRI-128 batch validation remain
required. PE2 authored this repair and cannot approve it. Item 1 and general
starvation remain in #2343. Keep #1521 open until merged containment and closing
regression proof. Worker attestation signatures are unchecked. No deployment,
GPU qualification, physical durability or campaign-completion claim is made.
