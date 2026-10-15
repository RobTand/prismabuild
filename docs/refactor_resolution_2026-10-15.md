# PrismaBuild architecture review resolutions

This records the engineering agreement with Claude and the remaining product
choices. It supersedes the corresponding positions in the
[disagreement brief](refactor_disagreements_2026-10-14.md). Agreement on a design
does not complete its implementation. The whole 31-card refactor remains open.

The [completion ledger](refactor_completion_2026-10-15.md) preserves all 111 original
requirements, corrects the historical credit, and names the remaining scope.

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
| D05 | Share stop, generation, singleton and marker operations in a kit. Each role keeps its loop and completion policy. Prewarm stops cooperatively, joins readers, aborts partial sinks and publishes accounting before proof. HTTP waits for actual request threads; return from `handle_request` is insufficient. | B05/B06 are integrated at `4cd546acec53`, with 410 focused passes and 94 assembled-source passes. The 384-file broad source check passed 5,358 cases with three recorded capability skips. Claude's final delta review remains open. Metrics is the sole observer; its supervisor owns generation retirement and the restart record. Explicit stop has a 10-second grace, and expiry permits no marker. Worker `--once` drift still exits 0. |
| D06 | Pure contract rows describe tag, declaration, enabled offer, companions and remedy. Gang and canary gates remain explicit. Host availability is separate from code support. | B07 source and engineering review are done. Extraction and separate tightening are integrated, including a 1,264-case combined check. Broader live eligibility, availability and submission ownership remain owed. |
| D07 | One placement view charges shared capacity, with separate measured-only and ledger-fallback fit queries. Preserve existing missing-evidence verdicts during extraction. Timer reads then consult existing deductions as a separate fix. Funding falls back to retired records only on ENOENT. Validate aliases per generation while preserving rename unions. | B08a/b are integrated through `25c0e98ae494`, including the requested runtime-tool removal and duplication check. B08c is integrated through `6ff48eb5bd87`, with 99 source-qualified cases. The assembled role/proof/funding check passed 303 cases. B08d generation-local alias validation and candidate-roster byte pinning are integrated at `30ca9a6f6041`, with 191 assembled passes. Historical membership-attestation manifest binding remains a separate follow-up. Final Claude delta review remains open. Timer changes prevent advisory waiting; no overcommit incident was demonstrated. |
| D08 | Share receipt facts and park-proof spelling in stdlib leaves. Unknown receipts cannot authorize new work. Physical proof needs a concrete current drain and exact process facts, independently of receipt availability. Keep named profiles and resign's additional checks. | Public names are integrated and qualified in the 303-case check. Strict leaf/member source `52b803d3bf63` passed 110 scoped cases; 17 host assertions remain deliberately deselected. The nonconsuming bridge passed 53 cases and 22 byte-replacement/recovery histories. These units remain outside the accumulation branch while prior-trust preservation, strict host consumption and typed receipt authorization are unfinished. Profile membership awaits Rob. |
| D09 | Use composable fixture rigs with production writers by default. Named negative helpers state the violated invariant and prove real consumer behavior. A schema-valid identity or publication defect is a legitimate negative fixture. | Original pilot migration passed 84 cases, with all 17 original assertions preserved. Ten remaining fake batch-writer sites then migrated to the real writer; all 238 original assertions remain and 41 cases passed. The fake batch writer is deleted at `60933914d4a3`. An action-free collision assertion now performs an actual damaged-ownership transfer; 36 cases passed. Eighteen normal binding setups still use fabricated scope controls, so broader fixture ownership remains owed. There is no blanket raw-write ban. |
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
success receipt, as expected. Skips are stated explicitly below.

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
| Fault-hit witnesses, `49f70fabc865` | 106 cases passed. All ten priority files received a source audit; executed witnesses remain distinct from source reasoning. Claude marked the audit done. | `a898990760b4c3be68ee5fdb77553b7894dc168a47a79c0c44448353505ceb82` |
| Registry producer controls, `5c2e246e8d92` | 84 cases passed, including actual canary and gang submission paths. | `5dcac3f3851dbcde52fb07e27d0d287a3097db1e3cc634968125453762fc581c` |
| Registry extraction comparison, `362fbeaf4d65` | 11 offers, 24 publications and five submissions matched the retained extraction baseline, with isolated imports and two write guards. Harness stayed outside the production branch. | `8a7c319007d359e1484a3809c99aba8953b7a6ea70cc9f84c8cbfc5f23b412a2` |
| Peer observation extraction, `c28f6aa8a414` | 27 fixed-input claim histories matched the retained base. Each side's imported modules came from its selected source. No concurrent-equivalence claim. | `ab9c30af8085b07f191ebd7e07d5fc37b191f8ee5a3d51f0e3d009429d45e207` |
| Peer timer correction, `bc610e2202e1` | 197 cases passed after two intended timer RED failures. | `42da8100da39e7edffff373bf4fddc7ca932a56dfc20ca03c674d4247709fa96` |
| Peer review delta, `25c0e98ae494` | 205 cases passed, including the duplication guard after removing the one-off runtime tool and stale B01 pair. | `d75e050b16a185a8201b64ce53903d430b86214d458c296aa8d3f1f7102dbda1`, `25b060f53b7ae8fbe75ecd03b7e270354e1c4e88998618bc1ee87827fe4e1769` |
| Role correction, `bdf7e7663f1a` | 410 focused cases passed, including real HTTP retirement/respawn, prewarm cancellation, generation fences and adoption. | `407de0b91631d8f75b712e7c3442c35405b11e4b09cab3b594668aef3df16c66`, `2e380e7bccc2e2cebb80c24fab19ae43f480f7219f6f863dd6e46348b4a8d174` |
| Roles assembled with prior owners, `4cd546acec53` | 94 merge-sensitive cases passed. Independent Git/AST checks preserved 1,318 unaffected paths and 45 role-only paths. All 43 source commits map to bounded integration units. | `c99bed6ea49a12ce0d5cf47bad31075e8036604d0138081b9229dae3d9686f5a`, `edc5abfbdc6896b299ffac97ba0b50e702c04a9aaec8719257bb887fc04d0307` |
| Funding fallback, `d3f7c595d1ce` | 99 cases passed after one intended zero-byte-live-file RED failure. Shared byte selection preserves each reader's existing encoding and size policy. | `b3d2d4d012c6a4709442ad57928509643a4de68d0aa5886f0f363dbe6910001c` |
| Role broad source, `bdf7e7663f1a` | 5,358 passed, three capability skips; 5,361 collected nodes. | `b7939cbedfc725c2e07db1e7b1b9d561b60ea7e386f5e672d3d4d10731685f0e`, `0e45970726da08686eb914d506cc35d3155593345bfc385970a80f8dbf62bef7`, `85b05ef5c6a395e27d280267c3cd8bcea4b3a91c168bd72130b311da86add11f`, `4aea6365c59ca863d9b8eaf2be49ecd7b373a0e68c99bf273bf384ece5209150` |
| Roles, public proof names and funding assembled, `34ae6fedc87e` | 303 passed, no skips. | `063acbc0f336452239a2cd1c8b952a467bf44142bbd97f2e42deef81071f1d0f`, `261858b04f734198c72f48e85a6f05109c86e9ae0dfc6971f1c2ac471d6971b3` |
| Alias and pinned candidate assembled, `30ca9a6f6041` | 191 passed, no skips, including the duplication guard. | `bf0c4310e7799c5a0421ad601d43b00bf945c98f7b8f7bdf42bafc12cedfa7a8`, `514c380497d7e2879199a4af8540f8bbc5a673c6cd005c74d5c182c7cb01a620` |
| Required proof identity and key collision RED, `3ba0fbfe4d92` | 28 intended failures, 16 controls; no setup failures or skips. | `9ede53811ad1baf4d8953a7eadc94812c58f937d4be716032a5bc95a07201b57` |
| Fixture pilot baseline, `34ae6fedc87e` | 79 passed across the three complete source families, no skips. | `df5c0a6aa37c189ef5c49093862d11ca0d39b8260fb454fc6081fad59e9329c3` |
| Claim histories, `27b3a2c4883d` | Six real claim, recovery, lifecycle and batch histories passed. | `906b2934b92e2770352c87deaff3508e6c72e29cea9960c16e44a138944d4785` |
| Reservation ownership, `8680d27b7063` | 336 passed across 19 files; independent review found no behavior changes. Integrated at `5ef153cf4bd2`. | `e1d0798c0c2eb89f4c6b134606043ac1bd7d739058fe99bd3b1bf0ec5f881c02` |
| Duplication test cleanup, `3f8023a19ec0` | Eight passed. One production scan replaces four; real scan and guard assertions remain. No speed ratio measured. | `89acb7f0b316916bf413a5bfdefb6f50b9b452acf173b5a0c87cc759a6ce7dc1` |
| Strict leaf/member proof, `52b803d3bf63` | 110 passed, no skips; 17 host-consumer cases deliberately deselected. | `30bbc674ef2ab2322446d067c4bf3d6bef7683f5e2661c9bbdcc403deacb0b35` |
| Nonconsuming installed-reader bridge, `f834a061b448` | 53 passed, including actual installed reader adoption and incapable-reader refusal. | `453bf0973aebdeabf4ac7ad47a057bd38bb2ff3c6843b6d495ca14731a246b96` |
| Bridge replacement and recovery cuts, `254417ba561c` | 22 passed. Fresh installed processes exercise captured bytes, forward cuts, rollback cuts and interrupted recovery. | `3d86fe29f3988896a74ecdf2a80cae7287beeca357e4cc88db3426217184fd61` |
| Spawned fixture broker, `265bb1446749` | 11 passed, no warning summary; actual startup, failure cleanup, affinity and bounded reaping. | `675e0e2520faace08b7ef29685fee1e1c2234874955ebf0610924bc9e0a259fa` |
| Original fixture pilots, `09ee7b1be50b` | 84 passed. Four fork warnings occur only in the retained concurrent CPU claimant case. Funding and broker lifecycle report none. | `83bb5e751e473b8ebb81072ed89a6c2b6c47b6995a1add6501da0f50ca41b61f` |
| Supersession baseline and B04 controls, `ff35d0bb7d31` | 36 passed. This characterizes the old behavior and is not a supersession fix. | `116a7b8c6278675f693430e8e0ad57f3e223a4ce903b03ca1fc6bc465a0cb9a7` |
| Real batch writer migration, `3090ea7e65a9` | 41 passed, no skips. Ten fake writes and their helper removed; 238 original assertions preserved. | `ceffe5a03143ffc76c3f7bc4a53bcc04bf4ea3d67d96d1f3a79095a95d8cb25e` |
| Cap test cleanup and progress fixture, `036a7e3777ae` | 59 passed after isolated denial proved missing worker capability tags. One tautological formula test removed. Initial 58-pass/one-failure result remains failed evidence. | `7a1212a6789a80ec70a8ebf247239b38adf91fbe451b0e5457bf62cc2978b4c4` |
| Actual token collision, `378c668c1cda` | 36 passed. Real transfer into conflicting held ownership preserves both holders, free tokens and the damaged total. | `1de2a723507f5513864a53211401b471bc4119bf25972a30f37ee457010619d7` |
| Worker receipt authorization RED, `6a966e0aa694` | Six intended failures, two controls; no setup failures or skips. Four unavailable/partial cases execute work; two falsely record drift. | `34582f473ed0aedd51f2f76513f60baeaa420d7989d7420995c5051723321e25` |
| Typed receipt observation, `156c5f71cef2` | 39 passed, no skips, including atomic pair publication and read counting. Caller migration remains unfinished and unintegrated. | `652615a37daa293c2e0a9c60cd0cf970e8eae62410cb7ec62f87c6544f3f377e` |
| Supersession corrected RED, `30733ed686d0` | 31 intended failures, 15 controls. The earlier invalid legacy fixture remains separately recorded. | `091478a3917d3f96533db9289c97bee33d0df1a43668f877045ac51d047d5227` |
| Supersession queue owner, `93c86e1ad595` | 39 passed, no skips. Direct tombstone settlement preserves the existing no-reinjection rule. | `f7a02bb8e2a5071fc20d264beb615a8e1856899608c6eae47c0a75c788682167` |
| Supersession reader and IPC, `f54a959c8290` | 150 passed, no skips. The campaign/canary wave and final review remain separate. | `cffbac421a4b5c4e7b9d9414c1455153c1aa2180f9d3a55e38305393318d5ad2`, `d2ff6f35050d4e56e0de5b3c96c34ca4177dbec51c4c0e68ea22dd6c629af076` |
| Prior proof trust RED, `fb7d8038ee09` | Five intended failures, three controls in admitted UID 0 container with inherited affinity. Both worker-controlled leaves gain root trust after rollback. | `40b87bb97c4f3fa456bed25cb62576a0214549df0d1cfc3f7e1ee648fffc6744` |

The combined 55-file cohort at `1943a388ecd1` passed all 1,264 cases. It covers
registry producers, peer placement and all ten priority fault files. Its verified
actions are `c9e1e522dc06dea274c11cda5d6257c0e2bf7734a3623ffad1412374972e047f`,
`dcd899e58363a69f3f718ce06ae44d4b7170d653dfde53886d6378e1f65d9d06`,
`c0cba87ee7537dd27604561b8af60456c7d7afcb32ae66f0222292b28f01eae6` and
`83b908e5f4cd9b8ee4aafc6eba6587861dd18d1de2d04c9d518bb0200b0d5aef`.

Evidence artifacts are under
`/home/rob/tmp/prismabuild-refactor-mailbox-20261015/`, in `manifest-phase`,
`requeue-generation`, `stage-marks`, `contract-registry`, `peer-placement`,
`role-session`, `role-integration`, `funding-fallback` and `integration`.
New unit qualification does not replace
the final integrated check or promote any deployment/workload-proof state.

The role broad check completed at `bdf7e7663f1a` with 5,358 passed and three
skipped nodes across 384 files. Two skips require a readable NFS mount; one
requires filesystem support for file-version proof. Six additional passing
subtest reports are not collected nodes. Earlier failed broad runs remain
failed evidence. The assembled role/proof/funding check at `34ae6fedc87e` passed
303 cases. Alias and candidate-byte pinning at `30ca9a6f6041` passed 191 cases,
including the duplication guard. Neither assembled check skipped a case.

The first alias RED had eight intended failures and two invalid-command fixture
failures, not ten intended failures. The corrected RED has 12 confirmed
failure-to-refuse cases and 19 passing controls. Candidate-byte pinning then
reproduced three intended failures with one passing control before its fix.
These failure records remain distinct from the passing implementation checks.

## New drain-identity finding

The old marker spelling maps both `1e-20` and `1e+20` to `1e_20`. Real-process
cases at `3ba0fbfe4d92` reproduce acceptance of the previous drain's marker in
both directions. The same cohort proves unknown required drain facts can be
accepted with an empty process census. Its clean result is 28 intended
assertion failures and 16 passing controls, with no setup failures or skips.

The proposed correction validates finite numeric drain facts, uses distinct
exponent-sign bytes, preserves ordinary decimal spellings, and rejects the
ambiguous legacy exponent spelling. Physical park proof stays independent of
runtime receipt availability. Strict leaf/member source passed 110 scoped cases.
The nonconsuming bridge and its fresh-process recovery cuts passed 53 and 22
cases respectively. The root consumer still needs prior-trust preservation and
strict acceptance qualification. Rollback must not turn a worker-writable proof
leaf into root-trusted code by copying it with stronger ownership and modes.
Typed receipt authorization is a separate unfinished caller migration.
The independent bridge review
and D11 source finding were sent to Claude in `20261015T030349Z`; no reply has
arrived. This is not a mutually closed discussion or a completed B09 card.

The real-root rollback RED at `fb7d8038ee09` reproduces the metadata loss for
both worker ownership and group write permission. Trusted rollback and both
ordinary repair controls pass. The first container attempt failed before tests
because its tooling snapshot omitted pytest's `py.py` shim; that is retained as
a setup failure. The corrected snapshot copies and verifies installed RECORD
files. No test ran outside PrismaBuild.

## Remaining requeue observations

An exact old-generation waiter still lacks a terminal result after generic
supersession. B16 will give it a typed supersession ending with a verified
successor and a distinct exit status. This is not an execution verdict or
permission to follow the successor. B04 preserves this existing limitation
while protecting the newer publication.

B16's corrected desired RED has 31 intended failures and 15 controls. Its queue
owner then passed 39 cases and its reader/IPC migration passed 150. These units
remain outside the accumulation branch until campaign/canary closure and the
untyped-terminal impersonation boundary are qualified. The new ending supplies
no execution verdict, receipt, resource release or successor-follow authority.

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
The reconciled index at `986cedeffae8` is committed and both reviewers marked
B12 done. It has one complete card, seven partial cards, 22 unfinished cards and
one product-choice card. All eight reproducible artifacts matched on root's
independent regeneration. This historical pin receives no credit for later
units. Card 22 is complete for its original structural and result-preservation
scope. Other unfinished requirements and product choices remain explicit.
