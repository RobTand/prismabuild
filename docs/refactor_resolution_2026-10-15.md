# PrismaBuild architecture review resolutions

This records the engineering agreement with Claude and the remaining product
choices. It supersedes the corresponding positions in the
[disagreement brief](refactor_disagreements_2026-10-14.md). Agreement on a design
does not complete its implementation. The whole 31-card refactor remains open.

The discussion is append-only in
`/home/rob/mailbox/claude-codex-prismabuild-refactor/`. Its pinned source is
`36cebfbcc189`, based on `6073c9d36555`. Newer source is named below.

## Agreed engineering choices

| Thread | Agreed design | Implementation owed or qualified |
| --- | --- | --- |
| D01 | Recommend retiring the old SLURM and Dagster adapters if Rob lifts the Phase 3 hold. The optional extra does not establish an external support contract. Preserve `slurm_lane`. The proposal incorrectly describes Dagster as rejected; the design selected it over Snakemake. | Retirement awaits Rob. Include all four dedicated test files if chosen. |
| D02 | `storage_tiers` owns mount discovery. A stdlib `stage_marks` leaf owns mark bytes, source identity and prewarm temporary grammar. Keep distinct `by_id_names` results and both reclamation exclusions. | B01 is integrated and candidate-qualified. Retiring `--stage` awaits Rob and a live-writer drain. |
| D03 | The initial-miss hook is qualification-only, but writes arrival and ready records. If retired, preserve its concurrency property with a controlled two-worker CAS-miss proof. | Hook retirement awaits Rob. The broader core/library extraction is still owed. |
| D04 | Model publication, execution slot, exact claim, reservation and evidence as distinct facts. A key may hold several generations' facets. Keep claim-pass rollback authority separate from persisted recovery facts. | B04 fixes requeue over a newer READY generation. Lifecycle classification and full conclude ownership are still owed. |
| D05 | Share stop, generation, singleton and marker operations in a kit. Each role keeps its loop and completion policy. Prewarm stops cooperatively, joins readers, aborts partial sinks and publishes accounting before proof. HTTP waits for actual request threads; return from `handle_request` is insufficient. | B05/B06 are in progress. Metrics gains an agreed 10-second stop-only grace; ordinary collection remains unbounded. Expiry permits exit without a marker. Preserve worker `--once` drift exit 0. |
| D06 | Pure contract rows describe tag, declaration, enabled offer, companions and remedy. Gang and canary gates remain explicit. Host availability is separate from code support. | B07 is in progress. Extraction precedes orphan/manual-tag tightening. |
| D07 | One placement view charges shared capacity, with separate measured-only and ledger-fallback fit queries. Preserve existing missing-evidence verdicts during extraction. Timer reads then consult existing deductions as a separate fix. Funding falls back to retired records only on ENOENT. Validate aliases per generation while preserving rename unions. | B08 is owed. The timer issue causes avoidable advisory waiting, not demonstrated overcommit. |
| D08 | Share receipt facts and park-proof spelling in stdlib leaves. Unknown receipts cannot authorize new work or certify drain. Keep named proof profiles and retain resign's terminal, reader-reference and census checks. | B09 is owed. Profile membership awaits Rob. Reader-first publication needs an explicit optional-member bridge. |
| D09 | Use composable fixture rigs with production writers by default. Named negative helpers state the violated invariant and prove real consumer behavior. A schema-valid identity or publication defect is a legitimate negative fixture. | B10 pilots and mutation checks are owed. There is no blanket raw-write ban. |
| D10 | Capture complete retained-reader import closures and verify their provenance. Run old readers with `python -I -S` and closure/stdlib-only paths. Check both reader directions and every writer variant. | B11 is owed. Supported generations await Rob. A retained directory is not proof of support. |

The detailed agreement and source citations are in messages
`20261015T003846Z-codex-to-claude-d01-d10-source-reply-3f753f.md` and
`20261015T004054Z-claude-to-codex-d02-d05-d07-d09-b04-b12.md`.

## Product choices still pending

Rob received three questions in this session. Their defaults are
recommendations, not decisions:

1. Retire the old adapters, experimental prewarm `--stage` and initial-miss
   rendezvous after their required checks, or retain selected paths.
2. Choose the roles required by host upgrade and member resign. Both agents
   recommend the work/storage roles, while metrics may continue. The supervisor
   and broker continue enforcing the boundary.
3. Choose the supported reader window. Both agents recommend live-offer
   generations plus an explicitly designated rollback target.

No retirement, proof membership or support window is inferred from silence.
No GitHub PR state or deployed behavior changed during this discussion.

## Qualified source units

All execution used published PrismaBuild entrypoints. For each action below,
the coordinator verified the source parent, terminal return code, immutable
stdout/stderr hashes, and CAS receipt and payload. Failed RED work has no
success receipt, as expected. There were no skips in these runs.

| Unit and source | Actual result | PB action |
| --- | --- | --- |
| Manifest phase selection, `de027746b4fe` | 68 behavioral cases passed. | `eeeda7eefec4633c15ba3c66b3e77dcf2ed79cd2d664f827e3c98520c50f41cc` |
| Manifest comparison, `de027746b4fe` | 45 manifests, 1,575 equivalent operation results against `6073c9d36555`. | `1fb9cb68267afd970c42b09359d5f4bd15d574d2697d83d0642064e7b471cd43` |
| Requeue reproduction, `3e157be54c1c` | Three overwrite cases failed; five controls/accounting cases passed. | `c28bdd67aea92a51224aaa57d295f080b588e6d95d6d235062eca3226065b5ea` |
| Requeue fix, `08a41806b04c` | 21 focused cases passed. Independent Sol and Claude source reviews approved. | `fa2895f802314c6cef455c0fd781ddeeca47884776f663caa2956b21bf337959` |
| Requeue integrated with manifest owner, `7be48621c2eb` | 195 queue/recovery/manifest cases passed. | `3c6c7b181c347a55d52fffe13ab6765bc23c4751aa8304afbc5e48891b71e7c7` |
| Stage owner and mount discovery, `ced76121a23b` | 235 cases passed. Independent Sol and Claude source reviews approved. Integrated at `8625ec0048d2`. | `7ed8306f56249d982e6a6cd1f9e4e395c8196c9e18dcfe820d2936b25ea23c88` |
| Manifest and residency comparison, `986cedeffae8` | 45 manifests, 2,115 equivalent results, including residency `remaining` and `accepted`. | `6f1fff5170bf933edd180cda2fd499e0f6af2d255e192323e8ab5cddcf3b45f5` |
| Requeue crash cleanup, `1284fa330db3` | 31 cases passed, including ten real-crash/cleanup cases. Claude approved. Integrated at `1a0e1dd92534`. | `65b5b096c0ec8a0325168a9eab99cf578aba98fc7a0994da1370e570fb95e03c` |

Evidence artifacts are under
`/home/rob/tmp/prismabuild-refactor-mailbox-20261015/`, in `manifest-phase`,
`requeue-generation` and `stage-marks`. New unit qualification does not replace
the final integrated check or promote any deployment/workload-proof state.

## Remaining requeue observations

An exact old-generation waiter still lacks a terminal result after generic
supersession. B16 will give it a typed supersession ending with a verified
successor and a distinct exit status. This is not an execution verdict or
permission to follow the successor. B04 preserves this existing limitation
while protecting the newer publication.

F1 cleans only aged, exactly named requeue staging siblings after their
authoritative tombstone disappears. Its qualified cases include a real process
death, retained fresh/live evidence, idempotence, ESTALE observations and intact
READY bytes. A tombstone retired by this sweep leaves staging for the next pass.

## Completion accounting

The full audit at `36cebfbcc189` has 31 cards and 111 requirement rows. Claude's
review corrects its counts to eight partial cards, 22 unfinished cards and one
product-choice card, with no complete cards at that revision. The earlier
version incorrectly credited unchanged helpers toward card 30 and over-credited
probe deduplication before the mountpoint helper moved. Later work is assessed
separately; the old audit is never retroactively credited with newer changes.

Frozen mailbox attachments are
`20261015T003738Z-codex-to-claude-completion-ledger-884dbe.md` and `.json`.
Their SHA-256 values are respectively
`8215aa65f95b86d57fdba7c63eb022ac3835ef4f8db003153c3a3472e8d285ed` and
`c065ae6b015333aedc858491f1f0206b07dd0f65ba9f8217400b8fed32f3d5e6`.
Claude's row review is
`20261015T010609Z-claude-to-codex-b12-ledger-review.md`. Its attachment has SHA-256
`8fbdaebe1ac20cb8119281341c95a1f850e5332c098093392ddb07d2023f4606`.
A regenerated index at `986cedeffae8` is in progress. Card 22 is complete for
the original structural and result-preservation scope. Test-fixture composition,
other unfinished requirements and the five product choices remain explicit.
