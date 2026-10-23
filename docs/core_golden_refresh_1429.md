# Core golden refresh for #1429

The opt-in lifetime request is validated in Core. Core changes from 305687
bytes with SHA-256
`d398c3541cdc26e808191dd98d1674bd9c9c3a33c320967ca4b9a3323ad5e461`
to 308862 bytes with SHA-256
`6f1917b9add29de8b560637547b69c62548a609e175aaae926387fe53315a9d7`.
`digest_primitives.py`, `immutable_files.py` and
`tools/prismabuild_worker.py` retain their exact source bytes, including
Core's immutable helper pin.

Six current Core receipt, attestation and runtime records were refreshed
through PB-admitted capture and strict runtime adoption. No golden or
`field_sets.json` was edited by hand.

Evidence is retained in
`/mnt/shared/pbtl-probe-tmp/pb-lead-dl380/1429-port/`:

- Capture action `d3a1cee76caebccbadd673b68610f971bb5095aba194b07c7f8421c60ade49a3`
  binds producer `4deeb06dfb353ccb458377179ab2cf1fa245953f`.
  Six current and twelve historical records were independently reproduced;
  both consumer directions passed.
- Strict adoption action
  `28aca6dec0de77d7a7b85e0e4d21c9e63716d12a74642f28d581f3297a3e1a1e`
  runs `refresh_compat_runtime.py --profile runtime` in its admitted checkout.
  It accepts only Core identity changes and their dependent digests.
  All twelve selected historical rows remain byte exact.
- `runtime-primary-audit.json` and `adoption-primary-audit.json` verify
  terminal status, complete cleanup, logs, source Git objects, requests,
  receipts and CAS result bytes.
- `integrate_verified_export.py` authenticates all five exported files
  against the audited adoption result before writing the checkout.
  It preserves 132 existing historical/control files and every unselected
  current row. Previous complete current files and provenance remain in
  `tests/compat/history-controls/superseded-runtime-54a45eeafcdd8d67/`.
- Five source-policy inventory additions were reviewed and adopted only with
  `refresh_compat_field_sets.py --before-source origin/main --review ... --apply`.
  `field-sets-review.json` and `field-sets-adoption.json` retain that review.

This refresh changes source identity and grants no deployment credit. The
SC-01 admission owner, ID-02 attempt identity, ID-08 runtime attribution and
SM-01 action lifecycle remain with their current owners. Source validation,
a person's settlement decision, runtime deployment and live timed-backfill
qualification are separate evidence; the latter three remain open.
