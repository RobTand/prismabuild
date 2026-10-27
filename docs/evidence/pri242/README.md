# PRI-242 integration of #2219

Qualified execution source is `8be219c5b1d878311e82fc38e47cf1eb3903d326`.
The final evidence commit changes only this documentation directory.

The integration merge retains approved PR parent `6a73af3edd365e8d10939bc8998f2dd527b4fa72`
and main parent `8be4d4056fbcd6285a932d3ac4bf5d8bf6015036`.
All 17 conflicts are recorded in `manual-resolutions.json`.
Both complete historical archives survive under distinct identities.
The ledger retains all 69 parent rows, every existing field and all whole evidence axes.

The integrated run recorded 560 passes and two missing-helper-import failures.
The import-only repair passes all 33 tests in its file. The other 29 files retain
529 passes, with identical file bytes and unchanged production trees.
Three exact upgrade-intent compatibility controls pass on x86 with the real
archival-replay capability. The final selection has 565 distinct passing tests.
The four mandatory guards contribute 112 of those passes. No test is skipped.
`qualification.json` records 25 audited test actions, including original failures
and the zero-selected action. A separate strict-capture action passed and its
primary audit is retained. Original failed evidence remains failed.

The first compatibility plan named the rollout partition and selected zero tests.
That action is unqualified. The corrected plan names the upgrade partition and
retains exactly the three required nodes. Both records remain in the packet.

`verify_qualification.py` checks terminal/log/source/file/cleanup applicability,
all 69 requirement axes and the exact final population. `audit_report.py` checks
full logs, complete native populations, source bundles, requests, immutable
execution receipts and payload hashes. Local result claims do not require a
reusable action-cache entry. Worker signing-key authority is not established.
`verify_preservation.py` checks both-parent ledger, assertion and archive retention.

The full packet and raw reports remain under `/mnt/shared/paperclip-runs/PRI-242/`
and are delivered on the Paperclip task. The two parent diffs are retained there.
Run the read-only verification on the coordinator:

```sh
python3 docs/evidence/pri242/verify_qualification.py --evidence /mnt/shared/paperclip-runs/PRI-242 --checkout "$PWD"
python3 /mnt/shared/paperclip-runs/PRI-242/verify_preservation.py
```

No runtime, cgroup, enrollment or service was changed. Independent native review,
PE2 intake, batch qualification and merge remain separate gates.
The source owners retain deployment, adoption and product acceptance.
