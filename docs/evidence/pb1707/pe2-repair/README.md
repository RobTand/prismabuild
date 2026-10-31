# PE2 repair evidence

The final [source qualification](final/README.md) records 2,419 passing checks, 123 verified success receipts, complete x86 queue compatibility and portable qualification on both architectures. Its [proposed merge acceptance](final/merge-acceptance.json) awaits independent Principal standards/spec review. The original checkpoints below retain their historical source scope and failed records.

# Initial PE2 repair checkpoint

Vehicle12 returned #2069 for two source integration failures. Both reproduced on the approved source. The structural refactor then retained those two failures while 97 checks passed. After the fixtures sealed their intended finite deadlines, 130 focused checks passed on the current-main composition.

The historical fixtures relied on the old implicit lifetime bound. The producer sealed no timeout, so it offered no finite drain opportunity. The host-fault case supplied only a worker ceiling, so its quiet allowance expired as `no_progress`. These are fixture contract mismatches. The original failed records remain failed. The two returned cases preserve all 22 original assertion ASTs and the existing deadline values.

`pool.py` now uses `_validated_action_request` for progress, deadline and advisory policy reads. Pool publication retains its `_sealed_action_request` error adapter. Execution and advisory classification share `_action_execution_timeout`. The default active payload still has no total deadline. The default sealed holder still has no finite drain opportunity. A true sealed deadline retains precedence.

`checkpoint.json` records source and evidence limits. `design-synthesis.md` explains the rejected API and scheduling changes. The audit files bind terminal records, native populations, logs, cleanup, request snapshots, result payloads and success receipts. Worker signing keys were not checked.

Rerun an audit on the coordinator with:

```sh
python3 docs/evidence/pb1707/pe2-repair/audit_reports.py \
  --report /mnt/shared/paperclip-runs/PRI-43/pe2-repair/repaired-focused.json \
  --head 3b732a3495549a1944b0fad0330a129763ad7ea6 \
  --output /mnt/shared/paperclip-runs/PRI-43/pe2-repair/repaired-focused-audit.json
```

The related export-allowance case also reproduced before receiving the same finite declaration. `assertion-comparison.json` and `compare_assertions.py` cover every original assertion in all three cases.

The first qualification at `237890e44c97` stopped after five x86 rounds passed 385 checks and its 19-file integration returned 410 passes and one failure. The original frozen plan and failed record remain unchanged. ARM was never submitted.

The failure exposed an error translation regression introduced by the shared-reader refactor. The unchanged sealed-timeout tamper test requires `ActionContractError`. Unchanged main passes it, while the structural-only validation split retains the `PoolContractError` failure. `tamper-disposition.json` binds those controls, identical test bytes and the original integration failure. Execution now reads through shared validation without pool publication error translation. The corrected final qualification is linked above. Independent changed-head review, acceptance and batch landing remain due. Historical qualification keeps its source scope, and all eight crash-era actions remain excluded. No deployment or whole-requirement completion is claimed.
