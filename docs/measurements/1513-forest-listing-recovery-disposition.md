# #1513: retained-evidence recovery disposition

## Decision and scope

**Unresolved historical comparison; stop at the approved-overlay/input prerequisite.**
This is the current continuation's durable report, not the inherited investigation
or a production repair. The earlier response left its disposition only in the
agent response; review could not find a submitted artifact. This document fixes
that delivery omission. Inspection base: `9dce28460e76d4115c925bac8efc4f397160f9cf`.
Only this report is added. Source, tests, assertions, fixtures, diagnostics,
permissions and retained records are unchanged. #1513 remains the tracking owner;
#1635's PR-hold exception is inapplicable. No PR, merge or publication is performed.

Terminology below distinguishes **inherited claims**, **verified retained bytes
and observations**, and **new executions**. Matching a digest authenticates those
bytes against the inspected descriptor, not an entire execution/admission chain.
No original cause, repaired defect or bounded non-reproduction is established.

## O1: bounded recovery inventory

Evidence root `E` is the supplied read-only recovery lead:
`/mnt/shared/prismabuild-fleet/investigations/1513-35e2bcfd/`.
Paths and resource values here describe retained evidence, not deployment defaults
or commands to reuse. Bulky artifacts remain in `E`, not in this change.

| Requested evidence | Recovery and adoption boundary |
| --- | --- |
| Reviewed head `f3d9e2bf9f3e56ff58270442ebdc1d1ddd1eb3ae` | Git report and inline diagnostic/test objects are readable. Its `docs/measurements/1513-forest-listing-investigation.md` reports **0/20 per source** and an admission denial, not completed comparison. No local containing branch/ref was returned. The inspected current base lacks that report and `tests/test_arbitration_forest_listing_diagnostics.py`. |
| Claims `386c1d5a9c15426a8c9f23d468187c4b`, `ce693af19cb747d2b3b65e9dbced3b7e`; review `b3458b94d6a6410481befd00c59e93a0`; later claim `d893ce237a6b4d46937e3010334768fa` | IDs and supplied summaries are provenance leads. The continuation's exposed `/conversation_history/` listing was empty; full run records and applicable execution provenance were not recovered. The inherited eight-pass summaries are **not** adopted as authenticated overlay acceptance. No denied recovery was retried in this repair. |
| Original failure | Readable `pb-queue/failed/7520710b811d8f6de735779cc77719183cc33473c735caec269aad24984a6ff9.json`, raw SHA-256 `876e0da54b5dd3e8325181cdcc5dec0a69676ff9a8ec2999743a8582b8f42a5c`, names snapshot `fa6139ea127899274f72c9919784a792dac94955` over candidate `96859b07…`. It reports the strict assertion failure, not a listing caller stack. |
| Newly discovered artifacts | Inventoried `investigation.md`, `verified-index.json`, archives/manifest, plugin, driver, collector, pair terminal/outcome/log records and success receipts; `o1.*` records also exist. The historical report/index claims are not wholesale adoption of their own verification assertions. `o1.*` is not established as provenance for the separately reviewed inline overlay. |
| Live PR/integration state | Earlier continuation's read-only `gh api` invocations could not start: `/home/rob/.local/bin/gh: Permission denied`, exit 126. Not retried without a changed basis. Live issue-linked PR state remains **unknown**. Local Git ancestry and absence from the inspected base do not prove live PR state. |

### Exact source and artifact identities

| Role | Revision |
| --- | --- |
| Historical main/control | `7fb7fa6b13868c33f82be3bf094448f06e99627d` |
| Original PR #1509 candidate | `96859b071f5e072defaf256c0466f65e7db8fed5` |
| Retained campaign's actually executed second revision | `661ea17e092c336409608769d7c608b924d8fb1a` (merged PR #1509) |

Git confirms the merged revision's parents are
`7c24107299e41d84f38cec7af4dbafd168a3015a` and the candidate above. Candidate
and merged `tools/fleet/stage_move.py` share blob
`5f034e7975b373df720bbac5945392f12f275384`; control has
`c5669be92da629b4352be9eadface74e4c980f71`. Candidate/merged full trees differ
across 39 files: **do not relabel merged-source executions as candidate executions**.

All three historical test objects are blob
`c1a81f31c1362ecb638257029d61d916b85fc8cb`. Both archived test files independently
match SHA-256 `f8b1e939c4818aa3878718af2a6363bfdecc18a79ef2be94f287f917d9ace869`.
They retain 2,000 names, 400 background namespaces and strict zero locked listings;
today's materially different fixture/refusal allowances cannot substitute.

The following raw SHA-256 digests were independently recomputed during recovery
and rechecked when persisting this report:

| Artifact in `E` | SHA-256 |
| --- | --- |
| `verified-index.json` | `e1bc894a3a19604c9dffe870fa6a764d92ce3c6ed50c1199b82883354d9371e2` |
| `forest_listing_overlay.py` | `bb34083dada496398e18db37ae98059589ee8ffdf8c7383245a06e13060e9a3e` |
| `compare_forest_1513.py` | `ab08d29fef1c5be5c30c1637379d0f51030a513305cd678f36069eba17b9ef67` |
| `collect_forest_1513.py` | `d57ca0c7042d4fa1490abef8904a050d98c839122c1855a498c76777cbdb5358` |
| `archives.json` (772 bytes) | `ce081d87c522f143563207fb919d6e17d431fb591be20dae36ae99e17da0ac3a` |
| Control `.tar` (23,746,560 bytes) | `25c8f381e1df334bccb4a8f5acb3c7a0660967f0147807b6340cc09a521de947` |
| Merged `.tar` (24,033,280 bytes) | `37d53e09982528e17f3fb2045383ad3c56dcb053461cbd0ae28a35798b8ec4bd` |

**The retained standalone plugin is not the reviewed inline diagnostic change.**
It wraps the historical observer's thread-local ownership predicate, captures
`listdir`/`scandir` caller summaries without live frames/source-line lookup, and
observes historical refusal APIs. Its incremental reporting requires more than
one stack; a single locked listing prints at session finish, not before the
historical assertion. The reviewed inline acceptance with injected listings does
not establish review provenance or historical applicability for this plugin.
No equivalent overlay has been recreated or applied here.

### Sealed-request/receipt sample: agreement, not full closure

Pair 01 action is
`a6a426783162581298084bc9e8dc3cf6864d6c2c990ecdb08799dcfc50b8778e`.
Its readable done record, receipt and retained request
`cas/requests/a6/<action>.json` agree on that action. Done reports one executed
attempt, return code zero; its referenced attempt is
`attempts/<action>/e3fac1b6e060516fc9d8def342c698a798f86c6eb95c28d1b822c558c2efacac/00000001.json`.

- Receipt embedded identity: `3e19f4ec7d54503936045f69abff20568779a11e095112fd2f8b829d93934528`;
  **raw receipt-file hash**, not the same identity:
  `aa8b062c9fde97c1b6ec9b55bfb85da10117b0c322fc9ed102513c2a1187636e`.
- Recorded action-manifest digest:
  `64b304c1896bb5803fe6ba0a66d4bd984322313f96735ac343769ef416c59564`;
  result digest `be7b1d80f1100ccd476d2a08aa5afe33f1160144d142a0b07a2959538980e6c9`
  (3,294 bytes). Embedded receipt-identity derivation was not recomputed here.
- Request/receipt name the archive manifest above and snapshot input
  `56a2d5e3c44c59026411f762f44abfc997a78b417d469286aa8348416806053d`
  (25,055,757 bytes), with snapshot commit
  `fed4e1484a82e63a44140011f6fb014a885c8443`. Recovery matched snapshot bytes
  to that digest; the object is a Git bundle, not a tar archive.
- The named payload is `tools/maintenance/compare_forest_1513.py --pair 1`
  with the archive manifest. Driver/plugin self-hashes in output agree with the
  retained files, but a closed bundle/source-to-actually-executed-driver/plugin
  chain is **not established**. Explicit `code_closure.files` enumerates a
  closure stamp, not those files individually; this alone proves neither their
  inclusion nor exclusion. No full bundle-tree/attempt/result/custody verification
  across all actions is claimed.

## Verified retained violation observations

Only the following bounded failure samples are adopted for caller attribution,
not as fully qualified O2 repetitions. Each failure record reports attempt 1.
Both stdout and stderr bytes/lengths match their failure-record descriptors.

| Sample | Action | Failure-record SHA-256 | Stdout SHA-256 / bytes |
| --- | --- | --- | --- |
| Pair 3, merged | `cb03911286637f31ac002af8dc696b798ab0dccb4f3760f78049248ca2005f50` | `5e4e40967266c4a9139585415cdc022750153019ad823c13e43e556f715ee07b` | `796480953f1d4a016065a6562dc45f795c23c27a8de2a504cb0fa999e195c293` / 238132 |
| Pair 4, control | `a027988ece1b8b2a98a874d6ab09a79a2c19328a383a9902eba174c6b77c8e9f` | `b71228c9cee8df721e572d4a0bff1e5fcaa338762344ca1e727b4580762ee18e` | `d3407453531916565832aef73dea1445a6575c2e457dd4c5305793b9c386683b` / 239007 |

Both 931-byte stderr files have SHA-256
`063ecbaec75cae1a0d4016370b2a96ca31e7512283d6080ee1c6a447d76cc80d`.
In **each** `pair-3.stdout.log` and `pair-4.stdout.log`, lines **358, 444, 530,
616** begin the four retained full paths and their caller stacks. These are the
references for every adopted locked path; the complete paths/stacks remain in the
digest-bound logs, not copied fixtures. Pair 3's path prefix is
`/tmp/forest1513-jrjz0d2e/661ea17e092c336409608769d7c608b924d8fb1a/pytest-tmp/`;
pair 4's is
`/tmp/forest1513-hkikjgy6/7fb7fa6b13868c33f82be3bf094448f06e99627d/pytest-tmp/`.

Stacks reach `_publish_divergent -> _arbitration -> redo -> _decide ->
_proof_search -> _shared_census -> _forest_census -> os.scandir`.
Merged `stage_move.py` lines include 2232/3119/3206/3289; control includes
2220/3057/3144/3227. The strict assertion fails in these samples and ordered
streams show immediately preceding same-directory refusals. This attributes
later retained listings to a caller path; it does **not** explain the refusal,
identify the original incident's caller, establish a PR-specific defect, or prove
harmlessness. A failed action's missing success receipt is not a pass and does
not erase its descriptor-bound failure logs.

## O2: counts, resources, deviations and terminal stop

The requested ceiling is **20 distinct executions per historical comparison arm**,
stopping an affected arm on its first violation. No new comparison action was
requested or executed on either architecture in this continuation or repair.

| Source arm | Retained x86 report/index claim: completed / failed | Authenticated applicable first-violation-protocol completions adopted here | New requested / completed | ARM historical result |
| --- | --- | --- | --- | --- |
| Control `7fb7fa6b…` | 20 / 7 | 0 | 0 / 0 | Unverified |
| Candidate `96859b07…` | Not established | 0 | 0 / 0 | Unverified |
| Actually retained merged `661ea17e…` | 20 / 8 | 0 | 0 / 0 | Unverified |

The retained report also claims 51 locked listings and complete receipt/log
verification. Those aggregate claims remain inherited, not certified by the
bounded samples above. Earlier continuation log inventory found 20 terminal and
outcome digest matches and 40 log descriptor matches, with recorded return codes
13 zero/7 nonzero (control), 12 zero/8 nonzero (merged). Such internal consistency
is not full sealed-input, admission, source or diagnostic-review authentication.
Zero adopted protocol completions does not mean the recorded executions did not
happen; it means they cannot certify this continuation's requested comparison.

**Resource/overlap evidence:** pair 01 request and outcome declare 2 CPUs/2 GiB;
allocation records CPUs 16–17 and 2,147,483,648-byte memory limit, with
`OMP_NUM_THREADS=MKL_NUM_THREADS=OPENBLAS_NUM_THREADS=1` in the request and output.
Its control target interval `1792504896.2667997–1792504950.5622203` is bracketed
by load `1792504896.259452–1792504950.723852`, which reports 298 iterations and
54.340507563 CPU-seconds. This supports actual temporal target/load overlap,
not continuous contention or a causal interference magnitude. The inherited
report describes all-x86 placement and 19 separately published pairs after pair
01 (aggregate declared 38 CPUs/38 GiB); those claims are not new admission proof.
Overlapping actions mean pair numbering cannot establish the chronologically
first violation. No x86 observation is extrapolated to ARM.

**Protocol deviations:** standalone rather than reviewed inline diagnostics;
merged rather than exact candidate revision; sequential source invocations with
separate load within each pair rather than the older four-pytest batch recipe;
and continued execution after violations. The old report's 4-CPU/8-GiB recipe
and deployment-specific interpreter paths are not commands authorized now. The
supplied environment instead admits plain payload commands through `run_tests`
with 2 CPUs/4 GiB, and forbids recursive submission.

**Exact unresolved prerequisites / disposition:**

1. Applicable review provenance for a verified retained historical diagnostic
   overlay is unavailable. Inline review does not establish approval/equivalence
   of the standalone historical plugin. Reimplementing it is outside scope.
2. The immutable historical source/archive/driver/overlay-to-execution chain is
   not closed. The supplied `run_tests` interface has no explicit historical
   input-attachment parameter, and action checkouts do not guarantee these Git
   objects or the external evidence mount. No supported immutable delivery of
   the verified historical inputs plus an approved overlay is established here.

These recovery/overlay prerequisites stop historical execution **before** an
admission preflight. No fresh historical admission probe was submitted, and no
denied historical entrypoint was retried. Current admission is not declared
unavailable from an old permission denial; a preflight alone cannot establish
overlay approval or input delivery. No runner, committed historical fixture,
permission workaround, coordinator-local test or nested PB submission was added.
Candidate and ARM comparisons remain unexecuted/unverified; established retained
violations were not replayed to seek a preferred outcome. **O2 execution is not
passed.** This is the authorized unresolved terminal disposition, not a request
for Rob to change host privileges. Leave publication and workflow transitions to
TrustLadder; resume only on a changed authorized evidence/input basis.

## O3: verification of this report-only change

Recovery used read-only Git object/ancestry/diff inspection and Python byte-hash,
archive-member and JSON/log inspection, not fresh diagnostic execution. Full run
record recovery, live PR state, complete sealed source/admission custody and
applicable historical overlay review remain explicit unknowns. The original
incident's cause remains unproved.

Repository guards ran through `run_tests`, with plain payload command:

```sh
python -m pytest tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

Both returned exit 0: **109 passed** on x86 Python 3.14.4 (74.67 s) and
**109 passed** on ARM Python 3.12.3 (30.43 s). Retained stdout was independently
reread with `read_test_artifact` for both operations. These are new repository
guard executions only, not historical comparison executions, overlay acceptance,
verification of inherited eight-pass summaries or evidence of non-reproduction.
The sole file in the final change is this report; no test discovery or coverage
was altered. Final whitespace/diff inspection found no source/test changes.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"b1940b2ca0370368733afcd95ec86b06970560858b2efe39b15f508565c1527a","size":1787}},"operation":"167b36d0c3373890ac866d24d220002888bf7c0f3d66b0adfc3b3b62d10d01d7","receipt":"7702e5e61a73a825f584524400be3837448bc339ef885ce0288e367dd53984f9"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"a25e0f905e28d0ed75665bd24144888dd325ba0b87c0a2be4868caa8a49d2c33","size":1790}},"operation":"9d11dbbff15a1353ccbdb2eab3c9de7e95a82eb1b69c2fbb831bc765216bf08d","receipt":"ee64b90b0d3b9882229e339e1bc48d9636d862a935774e2b9f46bec0d4296a82"}]
