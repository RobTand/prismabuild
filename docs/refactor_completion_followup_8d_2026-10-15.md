# C11 and C25 source follow-up

This assesses only C11 and C25 at
`8dacf78510df19bdb746f581b5b30a49e630625c`. Both now meet their four source
requirements. The full 31-card/111-requirement brief remains open. The frozen
[93424 audit](refactor_completion_93424_2026-10-15.md) is unchanged; its other
card assessments are not relabelled as a fresh whole-repository audit.

The requirement text comes from `refactor_requirements_93424_2026-10-15.tsv`.
Source references below all use the full 8d revision above. Qualification is
software evidence, with no deployment, physical ARM/NFS or complete workload
claim. Skips and the separate opt-in PB725 populations retain those limits.

| Requirement | Current ownership and evidence |
| --- | --- |
| c11.r01, version primitives, directory records and census memos in src | `src/prismabuild/kept_reads.py:443`, `:666`, `:914` own the Linux trust adapter, census memo and directory records. Fleet consumers no longer import those implementations from stage_move. |
| c11.r02, action reads untrusted unless retention is explicitly granted | `kept_reads.py:25` retains explicit file_version_scope. Fresh action reads and current filesystem qualification remain required; candidate names never authorize action success. |
| c11.r03, caller-facing filesystem trust port | `kept_reads.py:373` defines FilesystemTrust; `:443` implements the native adapter. Directory capture consumes that port. The original-path correction preserves symlink/.. observation identity. |
| c11.r04, shared capture protocol and changed-data reads | `kept_reads.py:589` owns detached acquisition, proof/candidate distinction, newest-acquisition publication, abort and revocation. `:914` and all metrics/census/history/checkpoint/range/forest consumers use it. Actual trust-loss, failed-acquisition, stale-publication and invalidation controls pass. |
| c25.r01, decomposition record locations | `src/prismabuild/decomposition.py:1062` owns record_dir and concrete record addresses. Campaign callers no longer assemble a second publication owner. |
| c25.r02, publish-once plan/index and parent/plan-bound readback | `decomposition.py:1102`, `:1124`, `:1167` own freeze_plan, publish_children and complete_group. Strict membership is captured at `:1225`; first-writer winner validation and canonical formatting behavior remain. |
| c25.r03, child membership and bounded result reads for every caller | `decomposition.py:1303`, `:1347`, `:1400`, `:1415` own native request/receipt-only lookup, bounded strict document decoding and caller-specific manifest handling. The1MiB cap precedes bulk result reads. Campaign null remains MissingChildResult; bound-query null remains ActionContractError. Queue/reporter grant no success credit for either. |
| c25.r04, private validators after caller migration | `_validate_plan`, `_validate_publication_index` and `_validate_child_result_manifest` are private at `:801`, `:1003`, `:1699`. Campaign, queue, reporter and tests use record operations. No public validator compatibility aliases remain. |

C11's isolated final 49-file run has 837 passes. Its original five actual
consumer REDs, separate path regression and corrected 29-node ledger behavior
packet remain recorded. Independent original/correction reviews and the
complete source replay are under `trusted-capture/` in
`/home/rob/tmp/prismabuild-refactor-mailbox-20261015/`.

C25's isolated final 38-file run has 1,155 passes. Its original eleven real
consumer REDs and paired bound-query-null regression remain recorded.
Independent complete-wave review and exact predecessor mapping are under
`/home/rob/tmp/prismabuild-decomposition-null-preservation-20261015/` and the
refactor evidence directory's `decomposition-query-null-20261015/`.

The combined 5ee source review independently reconstructs the whole pool AST,
every unshared packet byte, the design-document patches and the per-file case
plan. Its first full run failed two meaningful architecture checks. The 8d
correction removes the unused duplicate directory helper and raw digest site;
the legacy allowlist remains exact. Independent correction review verifies all
113 assertions/27 templates and actual 845-case focused pass.

Final full 8d qualification has 1,005 complete files and 14,843 unique cases:
14,819 passed, 24 unchanged skips, zero failures, plus six auxiliary subtests
passed separately. All twelve terminal/log/CAS chains verify. Every actual
snapshot's 1,431 tracked Git entries match 8d exactly. The full node set equals
the independent prediction; only the prior three explicit decomposition
repairs differ from the older full population, with all six replacement cases
present. Evidence is in `integrated-claim-capture-decomposition/` under the
refactor evidence directory, particularly `corrected-full-8dacf785/root-gate.json`
and `snapshot-replay/verified.json`.

C01 later phases, complete C05 ownership, C26 and the other remaining cards,
physical gates and Rob's product choices are still open. Source completion of
these two cards does not authorize a runtime publication or PR-state change.
