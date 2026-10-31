# PR #2363 integration repair

This record supersedes the original source qualification for the changed PR head.
The original [evidence](../README.md) remains historical. Engineer 10 must review
the repair independently before it returns to the batch merge queue.

The publisher checks canary placement before staging or activation. Its dedicated
preflight now suppresses bytecode writes throughout dry-run driver loading and
lazy hook imports, then restores the caller's setting on success or refusal.
Normal imports retain the caller's policy. Placement preflight and the original
source-tree equality assertion remain intact.

The import-boundary refactor is `405cd507981`, regression is `3a5f9abc19e`, and
behavior repair is `18741a0e090db54e4e5e59e711932d8e43010e49`.
Commit `2fb78badae2326a5da497b08a3764f65c1ec21dc` adds the strict current-writer
identity refresh. The evidence commit changes no production or test source.
The required rebase happened once before the original review. No further rebase.

## Reproduction and controls

All actions ran through published PrismaBuild clients.

| Check | Action | Result |
| --- | --- | --- |
| Original unchanged barrier dry-run ID at returned head | `e833ed92ebf0265119071553dc386b321762a96c09c1455dc49cd8bab51271b7` | 1 failed, pbcanary bytecode created |
| Placement tests after refactor only | `89e5dcf434e679dfbf1d7b5f0b2ac8948a1f1db97c90c014fbe915ea4582d39d` | 13 passed |
| New lazy-import policy regression before fix | `079675cdf8298cfefeaffceec9fcb4697d676e8fc794920206cc9c99855e72ab` | 2 failed, 6 passed |
| Disable bytecode during normal publication | `d746196fcd4446289afe780e62c0cb93684ff7f807f9c848b6b879c5681638bb` | 2 failed, 6 passed |
| Remove caller-setting restoration | `450727e5519167d334cb81875c39967ebf14729a20448c8d823efb2627570fbf` | 2 failed, 6 passed |
| Unchanged upgrade-intent/armed native golden ID before refresh | `bf7361b24b52a7ffbb755bf31332755bb202512f13a03016f39b80495daac3c6` | 1 failed, current writer changed golden bytes |
| Same unchanged golden ID after refresh | `3fd865a39799484b5a43092b9dfa0c0ea885b8bbe326d9305528181c0229ca0d` | 1 passed |
| All upgrade-intent compatibility IDs after refresh | `aa493af70b2e9f6d9b184b1bfd9c4ab06ff5cbbe2437db1e9b85f27a584b73e2` | 3 passed, 238 deselected |

No test assertion, reader, writer, strict adoption guard or provenance guard
changed for the golden repair. No test was removed or weakened.

## Exact fixture identity

The current intent embeds the executing publisher's compiled-source-verified
file hash. Changing the publisher therefore changes that identity. Capture action
`5a76704c19367dbf1a52c36186c71e3e2cd726c4e00da102327baae27d1bb5f4` independently
reproduced the current writer twice, reproduced two historical writes exactly,
and passed both reader directions.

The unchanged `refresh_compat_runtime.py --profile upgrade-intent` admitted only
`/coordinator_sha256`, from `43dc9a0fbf8f25a8ee8040bf6c3154b3983f3e36ac7fea82b4c5534a6d3d7287`
to the actual publisher hash `e1f50c3197e7b26ed56741b7cef7dc23a47072d923f84cc5c32c403bb0593d68`.
It archived the complete previous golden file and capture provenance byte for byte
under `tests/compat/history-controls/superseded-upgrade-intent-f1d6ce6052c13d54`.
All historical rollout bytes and the other nine current rows remain exact.

[Adoption](writer-adoption.json), [capture audit](writer-capture-audit.json),
[capture action and CAS paths](capture-action.json), and [fixture hashes](fixtures.json)
record the source binding. Superseded current fixtures receive no historical-generation credit.

## Fleet qualification

The changed test file ran five times per architecture at `18741a0e090`, twice
with two xdist workers. The production source is identical at `2fb78badae2`.
Each first run also covered the original barrier file and five neighboring
publisher or canary files. Distinct durations options prevent cache reuse.

| Source | Selection | x86 passes | ARM passes |
| --- | --- | --- | --- |
| `18741a0e090` | First run with nearby files | 115 | 115 |
| `18741a0e090` | Four further touched-file runs | 84 | 84 |
| `405cd507981` | Refactor-only placement | 13 | not run |
| `2fb78badae2` | Final placement and barrier | 29 | 29 |
| `2fb78badae2` | Strict capture/adoption intake | 8 | not run |
| `2fb78badae2` | Original exact golden ID | 1 | not run |
| `2fb78badae2` | All upgrade-intent IDs | 3 | not run |

There are 481 passing executions across 30 green test actions, zero skips.
Five expected-failure actions establish the regression and mutation controls.
The capture is a separate successful action. Every one has a verified terminal,
cleanup and stdout digest. The 31 successful actions have verified claims,
receipts and payload hashes. Worker attestations remain unchecked.

The broad compatibility-file action `516e127695a73cac9b6d57c249968e71ffd06d45b0ee319fca3520c6b76b8d2d`
exhausted its sealed 300-second execution budget. Cleanup completed and no receipt
was published. Its native completion is unverified and it contributes no passes.
[Timeout evidence](timeout.json) remains visible. The separate bounded selection
covers all three affected upgrade-intent IDs. No full compatibility-suite pass is claimed.

Validation used one native thread per worker, default eight pbtest clients,
priority -10 and 300-second action bounds. Qualification plus mutation units had
an aggregate maximum of 18 CPUs, 54 GiB and zero GPUs. Final integration declared
at most 6 CPUs and 18 GiB. The subsequent focused action declared one CPU and
3 GiB. These phases did not overlap. No measurement or runtime operation occurred.

The [test index](actions.jsonl), capture and timeout records name exact paths and
hashes. Readback queried precisely these 37 action keys through published read-only
MCP, with complete responses, no timeout or ambiguous terminal, a ten-second
per-call deadline and 1,000-line, 131,072-byte log tails. Full retained logs were
hashed separately. No whole-queue coverage claim is made.

Read the retained bytes again without submitting or executing work:

```bash
python3 docs/evidence/issue2285/return1/verify.py
```

Commands, reports, mutation patches, the fresh MCP collector and the downloadable
review bundle are retained under `/mnt/shared/paperclip-runs/PRI-173/return1/`.
The [scoped merge acceptance](merge-acceptance.json) separates source validation
from deployment. Independent changed-head review and batch merge remain pending.
