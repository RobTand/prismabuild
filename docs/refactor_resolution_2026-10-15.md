# PrismaBuild architecture review resolutions

This records the engineering agreement with Claude and the remaining product
choices. It supersedes the corresponding positions in the
[disagreement brief](refactor_disagreements_2026-10-14.md). Agreement on a design
does not complete its implementation. The whole 31-card refactor remains open.

The [completion ledger](refactor_completion_2026-10-15.md) preserves all 111 original
requirements, corrects the historical credit, and names the remaining scope.
The later [cc58 completion snapshot](refactor_completion_cc58_2026-10-15.md)
records its source separately from that historical ledger. The newer
[93424 audit](refactor_completion_93424_2026-10-15.md) preserves all 111
requirements and finds five complete source cards; the full brief remains open.

The discussion is append-only in
`/home/rob/mailbox/claude-codex-prismabuild-refactor/`. Its pinned source is
`36cebfbcc189`, based on `6073c9d36555`. Newer source is named below.

The accumulation at `09a4dd3f6c151f18f63a78f567c410a57755d6c9` includes the
ordinary installed package boundary and arbitration test cleanup. The package
exports its actual progress objects eagerly, loads its two broad module exports
on request, and keeps role receipt, drift, lock and park operations on their
stdlib owners. Its real installed-wheel tests physically remove broad modules
and verify actual operations, source origins, object identity and concurrent
first access. The integrated 164-file check passed 4,012 cases with eighteen
existing map_files capability skips; all 3,852 prior nodes remain. Four complete
source, terminal, log and CAS chains were checked, and independent review found
no actionable issue.

A meaningful frozen-namespace guard exposed a B04 omission. Commit
`f37413513af191f7d82ea712ce5793229b36e17d` gives the new supersession record the
`prismabuild.publication_superseded.v1` ID. The legacy allowlist is unchanged.
All 367 cases across thirty complete files passed, with both evidence chains
verified. No stored or deployed record migration is included.

The full baseline at `abf1516c81e86ccf5721abe5f42599ce3b1b7605` remains failed:
14,704 passed, 24 skipped and two failed among 14,730 unique cases across 1,000
complete files, with six auxiliary subtests separate. The failures were the
namespace guard and the large arbitration case exceeding its 180-second bound.
The latter also timed out in an isolated full-file run at `566892fc1dc7`, while
its other nine cases passed. Root checked every terminal, retained log and
receipt or expected receipt absence. A fresh 1,001-file run at `09a4dd3f6c15` is
pending; it does not retroactively change the baseline.

The arbitration regression now uses sixteen names and seven other namespaces.
Its real byte, ownership, refusal and filesystem-operation assertions are
unchanged. Three cases testing their own assertion helper were removed; one
real case was renamed with its bounded input. All seven retained cases passed.
Two isolated production mutations prove the smaller tests still distinguish
extra root listings and missed live-owner fragments: each produced six actual
call failures, including its intended real consumer failure. Independent review
checked the packing and retention limits, all four result chains, and the exact
rename/deletion map. This establishes the routine regression's coverage, not a
large-workload performance or measured flake-rate claim. Rerunnable source and
node audits, actual mutation diffs and evidence are under
`/home/rob/tmp/prismabuild-refactor-mailbox-20261015/arbitration-shape/`.

C01 T-qualification remains isolated at `503fb73a059ecd20a96af83c543b06a88df154d0`:
all 3,268 cases across 159 full files passed, retaining all 3,204 Unit A nodes
plus 64. The pre-H unit, decomposition operations and shared directory captures
are still in progress. C11's full 47-file baseline contains 811 passes and two
observed consumer assertion failures; those explicit tightenings and their
positive controls precede implementation. These checkpoints do not change the
frozen 93424 requirement assessment or close the complete brief.

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
| D08 | Share receipt facts and park-proof spelling in stdlib leaves. Unknown receipts cannot authorize new work. Physical proof needs a concrete current drain and exact process facts, independently of receipt availability. Keep named profiles and resign's additional checks. | Typed receipt authorization and the role caller migration are integrated through `32b79024ec9f`, with qualification and limits recorded below. Strict privileged proof and the retained-reader corpus remain isolated pending actual actor entry, native ARM/NFS and paired-host qualification. Their prior-trust repair is implemented and source-qualified separately. Ordinary installed package/role stdlib import closure is integrated at `566892fc1dc7`, with 4,012 passes and 18 existing capability skips across 164 complete files; strict physical-proof gates remain separate. Named-profile membership awaits Rob. |
| D09 | Use composable fixture rigs with production writers by default. Named negative helpers state the violated invariant and prove real consumer behavior. A schema-valid identity or publication defect is a legitimate negative fixture. | Original pilot migration passed 84 cases, with all 17 original assertions preserved. Ten remaining fake batch-writer sites then migrated to the real writer; all 238 original assertions remain and 41 cases passed. The fake batch writer is deleted at `60933914d4a3`. An action-free collision assertion now performs an actual damaged-ownership transfer; 36 cases passed. The final eighteen fabricated binding setups have since migrated to real ownership and causal funding/finish controls; the complete funding/broker file passed 45 cases and is recorded below. Repository-wide fixture ownership remains owed. There is no blanket raw-write ban. |
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
| Supersession reader and IPC, `f54a959c8290` | 150 passed, no skips. Subsequent caller and review qualification is recorded below. | `cffbac421a4b5c4e7b9d9414c1455153c1aa2180f9d3a55e38305393318d5ad2`, `d2ff6f35050d4e56e0de5b3c96c34ca4177dbec51c4c0e68ea22dd6c629af076` |
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
cases respectively. The root consumer still needs strict acceptance qualification. Rollback must not turn a worker-writable proof
leaf into root-trusted code by copying it with stronger ownership and modes.
Proof snapshot and rollback source `79c6a3669a60` now preserves UID, GID and
all mode bits. It rejects incomplete recovery metadata before service or file
mutation. The corrected root-container cohort at `e3bd5960d540` passed all 30
cases. Protocol-two positive readers at `61de730a6ae9` passed 79 native cases;
the affected real-root reader cohort at `82b45f3a203a` passed eight. Actual
`f834a061b448` protocol-one negatives and `30ca9a6f6041` adoption controls remain.
The equal-digest but untrusted-proof repair case and strict host consumers remain
unfinished. Typed receipt authorization is a separate caller migration.
The independent bridge review
and D11 source finding were sent to Claude in `20261015T030349Z`; no reply has
arrived. This is not a mutually closed discussion or a completed B09 card.

The real-root rollback RED at `fb7d8038ee09` reproduces the metadata loss for
both worker ownership and group write permission. Trusted rollback and both
ordinary repair controls pass. The first container attempt failed before tests
because its tooling snapshot omitted pytest's `py.py` shim; that is retained as
a setup failure. The corrected snapshot copies and verifies installed RECORD
files. No test ran outside PrismaBuild.

## Publication ending qualification

An exact old-generation waiter now receives an immutable typed supersession
ending when a validated newer READY publication ends it. The wait exits 76.
The ending supplies no execution verdict, receipt, resource release or authority
to follow the successor. Existing exact execution and withdrawal evidence keep
precedence. Campaigns stop their suffix while actual occupancy remains held.

B16's corrected desired RED has 31 intended failures and 15 controls. The queue
owner passed 39 cases, reader and IPC migration passed 150, caller closure passed
190, and the untyped execution-evidence guard passed 213. Independent review
reproduced nine failures and two positive controls at the demand, oversized
identity and cached-negative READY boundaries. Their fixes passed 139 cases.
The reviewer independently verified the result artifacts and closed all three
findings at `6932572a90a2`. The whole bounded wave is integrated through
`1e076b0d09c2`.

The combined source `961290a01b4d` passed all 677 cases across 48 files in four
PB actions. Root verified every terminal, source pin, retained log hash, CAS
receipt and payload. The cohort includes the reservation owner, exact readers,
withdrawal and preemption, campaign, canary, status and duplication guard.
These results complete this publication-ending unit. The full await module,
independent lifecycle facts and destination-based conclude remain owed.

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


## Assembled baseline and fixture corrections

The broad source `d8f8703a0d93` run covered 971 files in 32 PB actions. It recorded
14,093 passed, 80 failed and 26 skipped nodes. Six passing subtest reports are
additional reports, not nodes. All action evidence verified. Failed-action CAS
receipt absence is expected and does not turn a failure into a pass.

Three normal fixtures lacked the fixed progress/helper capabilities their
sealed actions require. A fourth campaign fixture advertised progress tags
without its canonical sealed declaration. Actual denial records reproduced all
four causes. Source `97db1b964c30` corrected four test paths and passed all 342
cases across 37 affected modules. Its 169 assertions, four exception checks,
function signatures and decorators remain, with the campaign key taken from the
actual sealed request. The correction is integrated at `4801e5aa4580`.

Six metadata-admission failures came from a shard admitting one CPU while the
fixture explicitly needs two inherited CPU IDs. The unchanged full file passed
21 cases with an actual two-CPU reservation. The status read-only assertion also
raced an asynchronous publisher started by its own fixture. Source
`d6aa04996bf6` waits for the actual returned publisher processes before taking the
read-only snapshot. All 14 test function ASTs remain identical and all 23 cases
passed. It is integrated at `961290a01b4d`. These scoped corrections do not
replace a final full-suite run on the completed source.

The typed receipt worker boundary passed 55 cases, including real unavailable,
repair and sticky-loaded-identity processes. Two assertion-preserving caller
cohorts passed 51 and 29 cases. The remaining production role callers, final
legacy API deletion and combined qualification are still outside the accumulation
branch. No deployment is claimed.

## Later capability, holder and placement units

The named-interpreter owner at `3697bf39290d` passed 146 cases. Dependency
eligibility at `4e2aba826156` passed 553 cases across twelve files and received
an independent source review without findings. Both are integrated, through
`92b9ad8e8b67`. Canonical dependency schema and probing owners remain unchanged.
Advisory file presence still cannot prove live claim-time digest equality.

The controller holder owner at `fe24b9451584` passed 228 cases and independent
source review. It is integrated through `b5ece98f0181`. The initial qualification
retains its one failed test assertion, which incorrectly indexed an exhausted
GPU kind omitted by the free-token map. The correction preserves production
and the acquisition, refusal, release and retry checks. Full ledger readers,
shared GPU sample facts and sampler injection remain separate requirements.

Placement clock ownership at `48d7baf845e9` passed 329 cases, following a
324-case baseline, and independent source review. It is integrated at
`b28d3e088389`. A queue-lifetime history feeds each pass view; both monotonic
graces and Unix diagnostic ages retain their read schedule and retirement
semantics. Three tests stop inspecting or seeding private dictionaries and
instead observe denial and repeat-claim behavior. The cc58 completion snapshot
is unchanged; it predates this clock requirement closure.

Image capability composition at `bc8c81c7b395` passed 733 cases across nineteen
files, following 717 baseline cases, and independent source review. It is
integrated at `a32b042861bb`. Omitted and explicitly invalid class reports stay
distinct. Advisory evidence cannot authorize a live claim, and the initial,
pre-rename and post-rename checks retain their current authority and timing.
The complete announcement caller migration remains open.

Three publication fixtures now bind real contained producers through the
existing broker and sealed request writers. Source `ce0354a825e6` retains all
253 assertion ASTs in the file, leaves other function ASTs unchanged, and passes
all 36 cases. It is integrated at `289b97371545`. Fifteen fabricated binding
callers remain at that source pin. These scoped results do not establish a
final integrated full-suite pass or completion of the original brief.


## Qualified source through 29d963e60265

The full ledger-holder reader at `c746c5f68669` passed 554 cases and independent
source review before integration at `09f803ae799b`. Measurement and scratch retain their
fresh second holder scans. Announcement composition is closed through `ed3e51a55e1a`:
all 42 callers across seventeen files migrated, the legacy public signature was
deleted, and 863 cases passed across 33 files. These extend the earlier partial
holder and announcement checkpoints above.

Core GPU facts first exposed a timestamp local shadowing the fairness clock
reader. The failed 42/477 result is retained. A separate production correction
renames that local, and a fixture correction gets seven new variants past
placement to their intended busy-room gate. The corrected closed wave passed
519 cases at `b1c512b4ce67`, with the independent finding resolved. Offer and
framebuffer projections at `bec2f9896483` then passed 534 cases in action
`0fea1b9aaadaa5dc3a5f573275a729c25773593820a20d91f4f03f354c336687`.
All previous 519 cases remain. The additional fifteen exercise public capacity,
first diagnostics, separate clocks, profile differences and retained/retried
windows. Independent review found no actionable issues; all four integrated
files at `29d963e60265` match the qualified source. Sampler injection remains
open.

Denial encoding at `b6918cc416ba` passed 204 cases and independent review before
integration at `fd3c61c60124`. The initial 181-case baseline exposed a staged-wait
fixture whose fabricated ledger lacked the real census API. A separate
fixture-only prerequisite uses the actual isolated ResourceLedger and passed
all 181, preserving the test bodies. The pure codec preserves wire values,
clock and exception ordering, ring/write sequence and memo behavior. A fresh
merged baseline at `fd3c61c60124` passed 575 unique cases across 35 files. Pure
room selection, staged drain notes and the reduced claim driver remain open.

ProgressWatch's complete stall rung at `654abddc197e` passed 476 cases across
21 files after a 471-case baseline, with no skips. Independent review found no
actionable issue. Integration `fecf54d2635d` preserves the qualified owned
function ASTs and test bytes. Existing ending paths, deadline credits and all
155 coordinator assertions remain. This scoped result still needs the final
assembled qualification.

The last fabricated binding callers in `test_prepaid_output_funding.py` are
removed. Unit A passed 41 cases; unit B's four independent refusal parameters
passed 44. The final race uses bounded causal barriers at real funding and
finish lock boundaries, with actor progress and exact terminal/token controls.
It replaces sleep/retry scheduling and deletes the three now-unused setup
helpers. All 45 funding/broker cases passed in action
`279f60151f3b8c4badcc2ade99a2fd45562c537aa3d45d7cad29e359b68f6b9d`.
Integration `187498e84cfa` is byte-identical to the qualified test source.
This establishes no measured flake-rate improvement or complete fixture-builder
closure across the repository.

Every action above has independently checked source parent, terminal, retained
logs and CAS receipt/payload. Populations overlap and must not be summed.
The cc58 completion audit remains fixed at its historical source. Typed role
receipts, the privileged proof consumer and broader RecordStore work are still
isolated or unfinished. No final complete-brief or full-current-suite result,
push, PR state change or deployment is claimed.

## Qualified refusal composition and flock owner

Candidate refusal composition at
`29bc2140b6aab1f07381b76a99ea543a65ed7221` passed 1,117 tests across 68 files.
All 1,098 baseline cases remain, with 19 new behavior cases. The moved refusal
classifiers retain publication, counting, room and withholding order. Measurement
identity and suspension observations still occur after denial publication.
Independent review found no actionable issue. The eleven integrated files match
the qualified source through `ad3898b87b2778cb45d4f20387ae2c82d0558aba`.
The subsequent test-only `bb8f41425b737df574a4cda3f9d0f79f31746ff3`
removes an exact truth-conversion counter while retaining the real falsey identity,
publication and history consequences. Its complete 19-case file passed. The
larger claim-driver decomposition remains unfinished.

The shared `flock_owner` module moves the existing procfs parser and holder
diagnostic without changing lock acquisition. Adaptive admission and role
singleton callers use that owner directly. The closed source
`1bce1ed3daf4eac81bb1e645a196eda1c287404f` passed 536 tests across 15 files,
retaining all 535 earlier cases and adding a mixed OFD/FLOCK positive-holder case.
Independent review found no actionable issue. All eight integrated files match
the qualified source at `54387776974f2d3efb31dd78de091ced7c21784b`.
This extraction does not establish the ordinary package or role import boundary.
The immutable file owner, record transport and package bootstrap steps remain.

Each qualification above has verified source, terminal, retained logs and CAS
receipt/payload. The corresponding `refusal-wave` and `flock-owner` artifacts
also retain exact old-node comparisons and source integration checks. These
overlapping scoped runs are separate from the pending final current-source suite.

## Qualified sampler closure and combined source

CPU and GPU controllers now accept observation callables through the public
claim entry. Their default collectors, read positions, caches and exception
boundaries retain their existing behavior. All 102 controller sampler mutations
are removed. The first complete 120-file qualification exposed 103 missed fixture
or caller failures. Those failures remain recorded at
`4fbf2cfcec247b8f92148e1e81a7147b9e129a54`. The corrected closed source
`637a60fbafd86d66f3f214e3b2b918bf728cb623` passed 1,892 tests, with the same
opt-in profile skip. All 1,885 baseline nodes remain, with eight new cases.
Independent review reconstructed the 92-file caller closure and found no
remaining issue. It also checked the merge against the flock and refusal owners.

Docker-shim tests now wait for actual client, late-start and cleanup events.
The fake client announces readiness after installing its signal handlers.
All 100 original assertions remain, and a new assertion requires both signals
in the real shim receipt. The complete file passed all 41 cases before and
after the change. No observed flake or performance improvement is claimed.

The assembled source `0e17e8b0ef2804b21a055f7e0d877b121138ece9` passed
6,619 tests across 434 files. Three skips remain: two require a readable NFS
mount and one is an opt-in profile. All 5,230 nodes in the earlier 356-file run
remain. Six auxiliary subtest reports are separate from the 6,622 unique nodes.
All eight source, terminal, log and CAS receipt/payload chains are verified.
This is a scoped integration run; the full brief and final suite remain open.

The duplication guard then exposed missing receipt-owner registration and stale
GPU/receipt helper entries. The correction registers only the distinct
`generation_receipt.observe` module contract and removes the exact obsolete
entries. The scanner and test assertions remain unchanged. Its full eight cases
passed at `c6a30113449057dc9839dd03ad31ce723147e2dc`, with verified evidence.
That source contains the assembled implementation plus only these data changes.
The accumulation branch advanced through that qualified sequence. The
`sampler-port`, `shim-test-signals` and `merged-sampler-flock-refusal` artifacts
retain the source audits, original failures, node comparisons and qualifications.

The publication-handoff race test now waits for the cleanup pass to finish
while publication is paused. It requires the actual busy-lock event for that
consumer, so a cleanup pass scheduled after publication cannot satisfy the test.
All 40 original assertions remain, with one new event assertion. Its full
16-case file passed at `8ad04d9799eda28f13dd47cc58c7ebd0ad1623c7`, with verified
source, terminal, logs and CAS receipt/payload. The `handoff-test-signal` artifacts
retain that qualification and the source-preservation audit.

The admitted descriptive inventory at that source found 999 test files, 8,883
test-function definitions, 96 sleep calls in test bodies, two reflexive-looking
assertions, 15 source inspections and no shadowed tests. These are review flags,
not executed case counts or failure verdicts. The two repeated-operation checks
remain because their second real call tests stability. Further hygiene work must
retain each test's actual behavioral consequence.

The combined qualification at `a230804e91b65011ccb8f082b770da77bd313c0b`
passed all 5,230 unique tests across 356 files and eight actions. It retains
every one of the 5,229 nodes in the earlier receipt, drain and mover runs.
The additional node checks `drain_policy.py` in the existing live-store
constant parametrization. Source, terminal records, retained logs and CAS
receipts/payloads are verified for every action. No test was skipped.
The `merged-receipts-drain-mover-verified` evidence and population artifacts
and the corresponding node-retention artifact preserve these checks.
This combined population covers those units; the complete brief and final
suite over all changes remain open.

## Claim captures and file-version context

Produced-output declaration, binding, live-owner, nonce, launch-context and
release operations now ask `RecordStore.claimed` or `queued` at their original
read positions. The questions retain plain missing/empty behavior and the exact
captured error. Each operation retains its existing error and identity policy.
The SDK's distinct empty-record behavior remains separate. The closed wave at
`747c580abe7e6f468453c2036b319091189552be` passed 924 tests with three recorded
skips across 57 files. All 920 baseline cases remain, with seven new file/fault
cases. Root and independent source review found no additional issue. The wave
is integrated through `bb549e09acd87d80842ce9f7112bbae08c616482`.

One skipped memoization test concealed a missing file-context argument. Giving
its trust check the actual path and replacing a fixed sleep with the shared
logical clock exposed six commitment reads where one was required. The same
missing context prevented stage resume from reusing a qualified parse when its
local device lacked a direct mount-table match. That new scenario failed while
29 controls passed. The callers now supply the actual path or still-open
descriptor to the existing trust owner. The unknown-mount control denies the
descriptor mount-ID observation too, preserving its unavailable-evidence case.
All 133 assertions in the three changed test files remain. The complete
five-file correction passed 96 tests at
`c7473f0a3f8c073608eb0a33abe93f01dd1ba018` and is integrated through
`234091f524ba84659ff820cf1df93a7e6547fbf3`.

The combined 60-file run at that integrated source passed 1,012 tests, with two
NFS-only skips and no failures. The former memoization skip is an actual pass.
Both full source, terminal, retained-log and CAS receipt/payload chains are
verified. The `merged-claims-file-context-verified` artifacts retain the result.
The wider queue-store closure, filesystem trust port and full brief remain open.


## Qualified receipt, drain and queue-reader steps

The current accumulation includes typed receipt and role callers through
`32b79024ec9f`, staged drain policy at `bfc31dd1f1a7`, and mover polling at
`7160eb7de875`. These extend the source-fixed checkpoints above. The original
31-card brief and the final current-source suite remain open.

Typed receipt observations require a complete commit/generation pair. Each
role retains its existing completion boundary. Worker Unknown waits still
record physical park evidence at both idle receipt boundaries while fencing
queue work. The public adapter keeps its role-specific drift and maintenance
behavior. All nine legacy scalar getters and their private alias are removed.
Independent review closed the missing park, stale caller and contradictory
documentation findings. The contained-child fixture now checks the exact held
DONE CPU allocation and containment within the broker and PB parent masks.
Its full 25 cases and both park-boundary files' seven cases passed.

The 272-file merged receipt qualification at `3f011ff2365a` collected 3,853
unique cases, with 3,852 passes and one incomplete tier fixture. The complete
seven-case file passed at fixture-only `32b79024ec9f`, preserving the original
cycle assertion and requiring an actual moved-runtime diagnosis in its negative
control. This layered evidence is separate from a final full-current-source
pass. C17's standard-library role-runner boundary remains unfinished.

Staged drain policy at `16764c8f3de3` passed 1,033 unique cases across 62 files.
All 998 prior cases remain, with 35 new cases and all 350 pre-existing
assertions in migrated files retained. Independent review found no actionable
issue. The integrated owned function ASTs and five other file bytes match the
qualified source, apart from the already integrated standing type annotation.

Standing Wave S at `a6435dd8660c` passed 602 cases. The expanded mover baseline
then passed 709 cases at `890c6acb00a0`. Mover Wave M at `817ed28c2ea9` passed
722, retaining all 709 and adding 13 real file/fault cases. Independent review
found no actionable issue. All ten production mover polls and every old helper
caller migrated. The remaining claim readers, queue I/O leaves, publication,
retained readers, liveness and prefix readers still need their own closure.

Every reported action has checked source, terminal, retained logs and CAS
receipt/payload, or verified expected receipt absence for a failure. The
populations overlap. The strict host-proof and root/reader qualifier work
remain isolated pending actual ARM and paired transport qualification. No
push, PR state change or deployment is claimed.


## Qualified pretransition wave and fresh audit

The independent 93424 audit is a full-brief checkpoint, not a completion claim.
Its five complete source cards are c02, c04, c08, c19 and c22. The linked ledger
retains every original requirement and separates source from qualification.

C01 pretransition wave 2 at `8e3222524f509d01c47e110397751819425a8ce4`
passed 2,096 unique cases across 106 complete files, without skips. All 2,067
prior nodes remain, with 29 added. All three source, terminal, retained-log and
CAS receipt/payload chains are verified. Independent review found no blocker.
Integration `3667f11dc2ed62a9aa0dbbe14276e0d6f385e74d` preserves all four
qualified changed files byte-for-byte. The retained scan reader and staged
pretransition/busy policy preserve conditional observations and their phases.
The driver still has 723 executable AST statements; six C01 waves remain.
The existing unbounded ready-GPU veto is unchanged.

Immutable-file owner P2's first complete 77-file run at
`16a44b327fd5bbdd154495ea299235ceae095756` had 2,626 passes, 18 existing
capability skips and one stale four-source assertion failure. That RED remains
recorded. The separate correction at
`8210201e7ab53472e547e4861785155e80c75f92` checks all five actual materialized
source hashes. The full rerun passed 2,627 cases with the same 18 capability
skips; all three source, terminal, retained-log and CAS chains are verified.
One approved textual source-count mirror was removed and seven real cases were
added. Independent review found no production defect and accepted the correction.

P2 is integrated through `3c9e73edf6f6e776471f176c56f874089a23c8b3`.
Thirty-two whole files match the qualified author source. The pool and design
overlaps preserve the current claim wave and later design correction; the
rerunnable source audit checks every pool function and the exact prose edits.
The complete 475-file consumer union at that source passed 7,741 unique cases
with 21 skips and six separate auxiliary subtests. All eight full chains are
verified. All 6,622 prior combined nodes remain; 1,140 more nodes mostly come
from newly included full files. This is a scoped integrated run, not the full
repository suite. Eighteen skips require privileged map_files identity proof,
two require readable NFS, and one is an opt-in claim-loop profile.

The receipt replay test at `5f03306cfcef15a7c2ec171516b88a2224bd4f16` fixes the
real file's timestamp before replay instead of sleeping for the clock. Its
complete 18 cases passed, retaining all 63 original assertions and node IDs.
The baseline file is byte-identical at qualified `0e17e8b0ef2804b21a055f7e0d877b121138ece9`.
There is no measured flake-rate or speed claim.
