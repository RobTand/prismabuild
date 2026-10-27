# PRI-243: #2363 integration qualification

The integration merges pinned Vehicle21 main `8be4d4056fbcd6285a932d3ac4bf5d8bf6015036` into the previously reviewed `a1bac460f128cbd7872349c8a679277efd8c485c` without rebasing. Merge `7585278f635e4e1d7cd7dc8eb324125b07969f40` preserves automatic merges. Strict capture adoption produces tested source `7fb8c362e9e50ab05ecd9e9b08c54f22368e10fa`. The subsequent commit adds only this evidence directory.

The three conflicts are recorded in `merge-tree.txt`. `merge-resolution.json` explains each manual resolution against both parents. Complete parent current golden, capture and historical files remain under `tests/compat/history-controls/parent-rollout-<parent SHA prefix>`. The canonical colliding `superseded-upgrade-intent-f1d6ce6052c13d54` retains main's complete bytes. Its `-a1bac460f128` sibling retains the PR parent's complete bytes. `preserve_parents.py` checks every preserved file against its Git parent.

The unchanged strict recorder produced the integrated writer's current bytes twice, reproduced the historical input exactly and checked both reader directions. The unchanged adopter changed only `/coordinator_sha256` to `62ff378a0288a42e81a87566fb57d50e89a26816c4cc2935108cc0fe909e8b60`. It archived the complete prior main golden and provenance. `strict-capture-qualification.json`, `capture-audit.json` and `adoption.json` retain the evidence. The authoritative scanner generated 592 field policies equal to the integrated inventory. Its optional full inventory completeness verdict remains incomplete and earns no qualification credit.

## Fleet results

| Selection | Passed | Action key |
| --- | ---: | --- |
| Placement and dry-run controls | 21 | `2a317fac0f5a67e7d52d063b3a7d552c406d90dc3f58641ec4ea3ebb53fdb2d3` |
| Activation boundary | 8 | `1029c47876b43da4d37372dbfab0c62fc5697dc7f8bbf208f4bfff9a72eb061c` |
| Strict capture intake | 8 | `c6e2e6e7640183eebc89ec4d1985176ab9679a7de87f33d118d9a31a9e6a331d` |
| Wire inventory guard | 8 | `d323105fde5d760fb87c3ae2146b245a890291aad0e1de2357970deeee271c58` |
| Fleet tool help guard | 95 | `1da0233c88cf57036aff8517f586c265145945df558f7cd3b898e03e59e17a51` |
| Duplication guard | 8 | `5b2b1cade79357abe6f1f46041f44bd182aba468fc70de902ad8e9b1440d5472` |
| Design reference guard | 1 | `36d7dfee11709cfc515a0e5866456de7140b429497f58dfe92248a9949bbb4de` |
| Native upgrade-intent and upgrade-attestation controls | 7 | `131322b1cb6e9a746df649154d6ca4771d5edd9db14fe2ceede773486b875ec8` |

Total: 156 passed, zero failed, zero skipped. The compatibility selection intentionally deselects 11 unrelated IDs. Strict capture action `b65406ddc95db56dc6ee4832a31d493d59835387fb0eede657d8c4b85c6fdb93` succeeds separately and contributes no tests. Every action ran on dl380g10 under ordinary CPU admission. No measurement was submitted.

`qualification-plan.json` retains exact published pbtest commands. One transient controller ran both commands sequentially with default eight clients. Every test action declared CPU1, memory4GiB, GPU0, native threads1, priority-10 and a 600-second payload cap. Aggregate ceiling: CPU8, memory32GiB, GPU0. Capture declared CPU1, memory3GiB and the same thread/priority/payload limits. Controller `pb-pri243-qualify-7fb8c362e9` is inactive/dead with Result=success, ExecMainStatus=0 and MainPID=0.

`qualification-audit.json` binds all nine terminal records, 18 complete log streams, sealed requests and source bundles, native test populations, result claims, CAS receipts and payloads. Every resource scope is complete, empty, settled and released, with no pending tickets. PB's generated closure files match their sealed manifest bytes. All other submitted source bytes equal the frozen parent. The compatibility report's null receipt path is resolved through its actual native ending and successful CAS receipt.

## Read-only replay

From this checkout, with the retained fleet evidence mounted:

```bash
python3 docs/evidence/pri243/preserve_parents.py
python3 docs/evidence/pri243/audit_qualification.py --evidence /mnt/shared/paperclip-runs/PRI-243
```

These commands inspect existing Git and fleet evidence. They submit no tests. The review packet includes reports, commands and full stream bytes. Large immutable CAS source bundles remain at the exact content-addressed paths in the audit.

The scoped merge record is `merge-acceptance.json`. The original PRI-173 component reviews, regression and mutation evidence remain unchanged. Its original broad compatibility action that timed out at 300 seconds remains failed and unqualified. This task changes no production behavior, test assertion, capture/adoption tool or historical bytes. No full suite was repeated.

Worker signing keys remain unchecked. This evidence proves scoped source/component behavior only. It establishes no deployed generation, live-host lifecycle, power-loss durability, canary, staged-only campaign or whole-requirement satisfaction. All ledger axes and R6 protections remain unchanged. Engineer 10 reviews the final published head independently. PE2 owns composed validation and batch landing.
