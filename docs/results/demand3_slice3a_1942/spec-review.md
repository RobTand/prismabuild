# Spec review — slice 3a #1942

Inspected base `88b1a5108799040c4dbd87bfd628a82e44bf739b`, implementation head `7cec101154de517b7ded399ca2adaf8afc33f8de`, and fixture repair `6cef1e12ef` using the actual diffs and source. No files in the reviewed worktree were changed. No tests or builds were run.

**Findings: 0.**

The four bounded topics map to the existing RecordStore paths and retain exact dotted/colon tier IDs. Claim order is embedded in tier commitments. Funding suffix parsing preserves the tier portion, including retired output funding. Tier reservation traversal covers private and hidden holders, token movement and rollback, and `minted/dead`. The coverage predicates reject unsupported IDs and unsafe or unreadable relevant scopes. Shared watcher paths retain watch-before-scan reconstruction, buffered invalidation replay, lazy/populated subtree handling, moved-away watch removal and overflow withdrawal.

This satisfies the design requirement to “Cover these mutation families through explicitly bounded namespace adapters” without adding record-body reads or scans under transition/admission locks. DirtyInputs retains unconfirmed IDs and the original absolute read budget. Contract documentation distinguishes actual announcement/acquisition/claim-order horizons from transport recovery and correctly assigns consumer arming to later integration slices. No consumer or live runtime is installed here.

The v2 fixture repair preserves refusal coverage. The partial case now removes `done` before capturing its cursor, then requests that unsupported topic. The negotiation fixture explicitly advertises only `done` before rejecting `tiers`; both continue asserting refusal rather than successful waiting. This removes the obsolete premise that tier topics must remain uncovered.

Limits: this is a read-only source/spec review, not approval, fleet receipt acceptance or deployment evidence. The configured independent external reviewer gate remains required.
