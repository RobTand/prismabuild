# Function-level test selection

PB's TIA tools record calls by pytest node ID, compare Python ASTs, and emit
pytest targets with a reason for each selected file. TIA remains opt-in. Keep
full queue tests. Do not enable TIA shadow mode until the review fixes, at least
100 valid mutation comparisons, and a map refreshed on hosted runtimes are
accepted. The tools do not change
CI workflows. PB's CI uses GitHub Actions on GitHub-hosted runners, with Mergify
as the merge queue. The lead owns that integration in #2433.

## Recorder and map

`tools/ci/tia_record.py` is a stdlib-only pytest plugin for Python 3.12 or
later. It reserves an unused `sys.monitoring` tool ID and listens for
`PY_START`, `PY_RESUME`, and `PY_THROW`. Start and normal-resume callbacks
return `DISABLE` after their first event per code object and boundary. Exception
resumptions use the global `PY_THROW` event, which cannot return `DISABLE`.
Test boundaries and fixture
boundaries call `restart_events()`. The recorder keys functions by repository
path and `co_qualname`, including nested functions and methods.

Only calls in `src/` and `tools/` are recorded. Module code is excluded.
The plugin hashes source files before tests start, so callbacks do no filesystem
I/O while tests run. Child processes reuse a parent-written catalog, avoiding a
repository scan at every Python startup. The digest and JSON recipes come from
PB's existing stdlib digest owner. Shared fixture setup carries dependencies observed in its parent process to
every consumer, including tests that reuse a cached session fixture.

The plugin removes its own `PYTEST_PLUGINS` entries from child environments.
It propagates explicit plugin arguments to xdist workers instead. A child that
replaces `PYTHONPATH` therefore does not inherit a broken pytest plugin import.

The plugin creates an opt-in `sitecustomize.py` in its output directory and
prepends that directory to `PYTHONPATH`. Python subprocesses that inherit the
environment record under the parent's `PB_TIA_NODE`. Each process writes its
own UUID-named JSON file. Fork children reset their process records.

The environment variables are:

| Variable | Meaning |
| --- | --- |
| `PB_TIA_ROOT` | Repository root, resolved before recording |
| `PB_TIA_OUTPUT` | Process record directory |
| `PB_TIA_COMMIT` | Source commit for this population |
| `PB_TIA_NODE` | Parent test node, maintained by the plugin |
| `PB_TIA_CATALOG` | Prehashed parent catalog, maintained by the plugin |
| `PB_TIA_EMIT=1` | Emit gzip/base64 records to CAS-captured stdout |

`tools/ci/tia_map.py` merges records into `prismabuild.tia_map.v1`. The gzip
JSON contains a sorted function table, integer function IDs per test, file
SHA-256 hashes, the source commit, and observed Python versions. Sorted tables,
compact JSON, a zero gzip timestamp, and a normalized gzip OS header make
repeated merges byte-identical across supported Python runtimes.
Mixed source commits or conflicting file hashes are refused.
Functions executed outside test boundaries are recorded separately, without
attributing module execution to tests. A change to one of those functions
takes module fallback, even when other tests also called it directly.

The nightly refresh must run the complete gate population with both
`tools.ci.tia_record` and `tools.ci.tia_population` in `PYTEST_PLUGINS`.
The second plugin applies the gate's unchanged node quarantine with pytest's
prefix semantics, including every parameter case of an excluded test. PB's `pbtest`
partitions development map runs, with at least 4 GiB per shard. CI's nightly
refresh runs on GitHub Actions and does not submit through PB.

A refresh records only shards with a complete passing test verdict
and a verified CAS receipt for development evidence, or the equivalent complete
CI artifact. `tia_evaluate.py extract` refuses incomplete development reports.
`extract-cas` verifies result blob hashes and merges records inside an admitted
action. A known baseline failure can instead supply its verified terminal-log
bytes and explicit `baseline_failures` metadata. That input has no CAS receipt
and is never reported as a passing shard. Unmapped test files retain the static rule until a refresh records them.
Their metadata names the reason, observed hosts, and action keys.

### Refresh procedure

1. Freeze the source commit and gate population, including its quarantine. Use
   clean committed worktrees for development actions. Include unchanged gate
   helpers in that execution snapshot when the gate PR has not landed yet.
2. Record every supported Python and architecture configuration. For development,
   submit the population through the published client, with no placement tag:

   ```sh
   python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py \
     --checkout <clean-worktree> --python /home/rob/venvs/pb-cpu/bin/python \
     --workers-per-shard 1 --threads-per-shard 1 --mem-gb 8 --priority -10 \
     --timeout-s 3600 --test-timeout-s 180 \
     --env PYTEST_PLUGINS=tools.ci.tia_record,tools.ci.tia_population \
     --env PB_TIA_ROOT=. --env PB_TIA_OUTPUT=tia-records \
     --env PB_TIA_COMMIT=<frozen-source-sha> --env PB_TIA_EMIT=1 \
     --json <report-outside-checkout> <gate-population-files>
   ```

   `PB_TIA_ROOT=.` resolves inside the worker's materialized checkout. It must
   not name the coordinator's original worktree. Set `PB_TIA_GATE_DIR` when
   the unchanged helpers are outside the default `tools/ci` directory.
3. Check every shard's terminal verdict, logs, receipt, and result hash. Rerun
   execution failures with corrected settings. Keep explicitly authorized
   baseline failures and unmapped files in metadata, never as passing shards.
4. In a portable admitted action, use `tia_evaluate.py extract-cas` with
   `PB_TIA_CAS_INPUTS`, `PB_TIA_UNMAPPED`, and `PB_TIA_BASELINE_FAILURES`.
   `PB_TIA_EXTRA_DURATIONS` supplies only missing gate costs. Preserve the
   verified input manifest and map digest with the gzip artifact.
5. Publish the complete map as the nightly CI artifact for that source commit.
   The GitHub Actions refresh runs pytest directly on its hosted runner with
   short private scratch outside Git and cleanup in a trap or `finally`.
   The lead owns this CI wiring. Refresh after test, fixture, configuration,
   or import changes. Enable shadow comparisons only after the review acceptance
   conditions above are met; retain full queue tests throughout.

## AST changes and targets

`tools/ci/tia_diff.py` compares the merge base to the head. It reports changed,
added, deleted, and structurally renamed functions, module changes, and
non-Python paths. Function ASTs include signatures and decorators. Formatting
and comments do not change the AST. A nested body change includes the enclosing
function. Invalid Python takes module fallback.

Definition-time expressions matter. Changed defaults, annotations, decorators,
class bases, assignments, and imports take module fallback because their effects
can happen before a test starts.

`tools/ci/tia_select.py` emits `targets`, suitable for pytest, and file rows
with `nodeids` and reasons. `nodeids: null` means run the complete file. It
always retains the fast tier and changed test files. Function matches select
individual recorded nodes. Fallbacks select complete files.

| Change or evidence gap | Selection |
| --- | --- |
| Recorded function | Tests that executed that function |
| Module code | Every observed consumer of the module, plus its static rule |
| Function used outside tests | Static rule and observed module consumers |
| Missing function | Static rule for the module, plus observed module consumers |
| Map hash differs from the base file | Static rule, plus observed module consumers |
| Non-Python path | Gate's static path rules |
| Source reader or test helper consumer | Gate's literal-path and transitive helper rules |
| Selection infrastructure or test configuration | Complete population |
| Test file absent from the map | Complete file whenever its static rule selects it |

Hashes are compared with the changed file's **base** content. Comparing with
its changed head would invalidate every useful function match. The source
commit is provenance; a map from another commit is usable only where the base
file's bytes match its recorded hash.

Mapped functions can still affect tests that read their source without calling
them. Literal-path readers and their transitive helper consumers retain complete
file selection from the static rule.

The selector imports `gate_select.py` for population, fast tier, quarantine,
static dependencies, and estimates. `--gate-dir` can name an unchanged copy
from the gate branch while that PR is separate. `tia_evaluate.py replay`
imports `gate_replay.py` for the static comparison and percentile calculation.
It does not maintain a second static selector or replay implementation.

The estimate remains the gate's whole-file cost, even when TIA chooses a subset
of nodes. It is a conservative file-cost estimate, not a measured node runtime.
Missing durations remain unknown unless the map supplies a measured fill for
a file absent from the gate duration table. A fill never replaces an existing
gate measurement.

## Evaluation status

The development evidence and acceptance limits are recorded below.
No CI job relies on TIA yet. Keep the queue's full population. TIA shadow mode
is also withheld until the review fixes, 100 valid comparisons, and a hosted
runtime map refresh are accepted. The historical replay does not demonstrate a speed benefit.

The repeatable evaluation tool has these commands:

- `extract` and `extract-cas` merge receipt-backed population records.
- `replay` compares the latest 150 merged PRs against the same current
  population as `gate_replay.py`. Historical base hashes must match; a newer
  map does not silently claim historical coverage.
- `campaign` emits a `pbcampaign.py` manifest with one mutation per row.
  It chooses deterministic, covered functions in `src/prismabuild/` and
  `tools/fleet/`, interleaves both groups, and limits static estimated serial
  cost to 1,200 seconds per row. The cheapest eligible static selection was
  912 estimated serial seconds across 195 files, so a 30-second cap admitted
  no mutations. This still biases the sample toward cheaper modules.
- `mutation` runs a clean baseline, then the TIA targets and full static
  selection against the same mutant. It restores source bytes and records
  failures, collection errors, counts, skips, and incomplete runs separately.
  Baseline failures are valid only when every failing node is explicitly named
  in the map's known-main-baseline metadata. Other baseline failures invalidate
  the row. Kills count new failing nodes relative to that baseline. A valid
  execution requires exit 0 or 1, JUnit, and a matching
  `prismabuild.tia_completion.v1` record from `tia_completion.py`. The record
  must show completed collection, every collected node reaching teardown, no
  collection errors, and no interruption. Partial JUnit is insufficient.
  Exit 2 never establishes a valid comparison. Mutation-caused collection
  failures are separate `tia_collection_killed` and `static_collection_killed`
  evidence, after a valid baseline; they do not count toward valid comparisons.
  Timeouts invalidate the row and never count as kills.
- `overhead` runs five interleaved plain/recorded pairs of the fast tier in one
  PB measurement action. It records Python, observed load, exit codes, and
  timings. Tests assert no timing threshold.

All development commands that execute tests, build maps, replay history, or
measure performance run through the published PB clients at priority -10 from
clean committed detached worktrees. Mutation rows request one CPU and 4 GiB
and bound native threads.

The readiness lead changed development routing after the 24 GiB dl380g10 temp
dataset filled. Published `pbtest.py` uses its default scratch handling, with
**no `--basetemp` override**, per the lead's correction. An earlier explicit
relative-root refusal collected no tests and is not code evidence. Direct
pytest invocations inside admitted actions use a short private directory
outside Git: `tempfile.TemporaryDirectory(prefix='pbm.', dir='/tmp')`, then
`--basetemp=<private>/t`. `TMPDIR` is also private to that arm. The context
manager removes the arm's data on success or failure. The lead authorized
this convention after the diagnostic below. No direct pytest command uses
an in-checkout `.pytest-basetemp`. The overhead runner and nested recorder
fixtures use the same convention with separate private prefixes.
Nightly map refreshes need a short private root outside their checkout too.
Published `pbtest.py` receives no basetemp override.

The initial mutation campaign was discarded after ten dl380g10 actions failed
with disk-quota errors. Those failures have no CAS receipts and count as
neither kills nor selection misses. Examples are
`162605357a67df8875de4aca4fa4e0398677ddd88173937c939df6e3c7d09c66`
and `43fa8eb328c63ceb5cbb6bb6d99c246cc7294688293f0485328eab00da208a48`.
The corrected Spark campaign exposed a second execution problem before
mutation: `.pytest-basetemp` beneath the materialized Git checkout changes
Git discovery, remains under the live-checkout guard root, and makes a UNIX
socket pathname too long. Diagnostic action
`eb23ef1596f15baadaad5e3d56bbfcd57d5853929c111df348007e10f3161e47`
on sparklina reproduced these three baseline nodes:

- `tests/test_live_store_guard.py::test_a_materialized_checkout_lands_under_the_guard_root`
- `tests/test_pbrun_placement.py::test_a_non_git_checkout_cannot_fall_back_to_mutable_execution`
- `tests/test_produced_output_lifecycle_r2.py::test_full_nonempty_lifecycle`

Its verified terminal stdout SHA-256 is
`b7faeeb60c26f733cef5bac29d21501aa42d090da94ab4a9040be1b2ec9f935a`.
It has no receipt. The active invalid campaign was withdrawn. These nodes
are execution-layout failures and are not added to the map's authorized
known-main-baseline list or removed from either evaluation arm.

Failure-heavy mutation resubmissions use `tags: ["gb10"]`, with the declared
dependency reason "dl380g10 temp capacity". Remove `anywhere` when adding a tag:
PB refuses that contradictory combination. PB places those rows among eligible
Sparks; no individual host is pinned. After the readiness lead's placement
update, map merges, replay, and ordinary verification use portable placement
with no tag. Architecture-specific repeats retain their architecture
requirement. Portable spool verification uses `PB_TIA_SCRATCH_ROOT=/home/rob/tmp`
to keep a short private disk path outside Git and outside dl380g10's tmpfs `/tmp`.
No agent cleanup removed anything from the shared `/home/rob/pbt17` tree.

The map freezes main at `90332a0eb844e46920593eb61efeee0a2c63905b`.
Its gzip is 1,230,608 bytes and contains 7,796 function identities and 19,701
pytest nodes, recorded on Python 3.12.3 and 3.14.4. It covers 1,247 test files.
The recording population had 1,248 files after the two authorized development
exclusions. Recorded nodes cover 1,240 frozen-main files and seven experimental
gate/TIA test files as a superset. Replay
and mutations use frozen main's actual population, with unchanged gate helpers
and TIA tooling isolated in `evaluation_support/ci/` outside inventory scans. `tests/test_compat_bundle_cache_isolation.py` contributes no nodes
because the gate already quarantines every node in that file.

The accepted build used 8 GiB, one-CPU population shards on dl380g10, sparky,
and sparklina. Seventy successful source actions have verified terminal records,
CAS receipts, and result blob hashes. The terminal log of one known baseline
failure supplies the remaining six-file shard. From the first accepted source
publication to the merged-map receipt, elapsed time was 1,326 seconds, including
completion runs and corrections. The longest original shard took 685 seconds
of pytest time. This elapsed build is development evidence, not a prediction of
nightly GitHub Actions time.

The map merge action is
`5b36deacf2885904c38fa657fcb180291c6f198c8d2d1ad7ead0b1fc41812a56`,
with verified receipt digest
`5108a9713e88680d0e5d4f90145636908e713d6ef3886e7d8ccc3fa5ccb53fcb`.
Its CAS stdout contains the gzip as `tia-map: BASE64`.

The canonical container was rebuilt from the same verified recorder inputs in
Spark action `ef3c8b0984856a026df26ef2f8110e3e0d2bdec8e3f466e643ca32c11816541b`.
Its receipt digest is
`44e07d08736c1de9d90d75d966b30063cd414e2c623eac1c61721e79b6975f58`.
The gzip remains 1,230,608 bytes; decompressed bytes exactly match the original
map, with logical SHA-256
`9397ff38125a3e7f4f055278d4d37b3605c30a1bfacdb9d16e2402058c772c17`.
Its compressed SHA-256 is
`28cd08a51de0904f456eefec49fbfeaf78e66e0000bfc824656863b845658796`.
Python 3.12's `gzip.compress(mtime=0)` had emitted OS byte 3, while newer
Python emitted 255. The writer now uses `gzip.GzipFile` with an empty filename,
which emits 255 on both. Portable red action
`2178d2edb92f3e4e850774b2efe7a940a2052ecbbf0f361841c6587a3c4288f6`
failed with `assert 3 == 255` before the fix and published no receipt.

The known main-baseline failure is
`tests/test_duplication_baseline.py::test_same_name_helpers_only_shrink`,
action `315ebd564f4a2ff7a01cd6f3ca42f699d202ef299e767da2f5482ee846157aed`
on sparky. It reports the existing `check_pull_request` and `issue_numbers`
collisions introduced by #2431. The readiness lead is repairing those outside
this footprint. The terminal stdout hash is
`5e736b2209f0ebb943b047cde1af6ad2c27ed75e15c335963822a89ab69f737b`.
It has no CAS receipt and is not a selection miss.

Initial contract evidence at `47d56296`:

| Contract suite | PB action | Outcome |
| --- | --- | --- |
| Missing implementation | `703bf93248f38dff934b0b2fddf8c0ab1a6fc5b311c154eef012b1f177492a35` | 11 intended failures; no receipt |
| First implementation | `1b30d4f9ea896dff6c55ba60b4f56113e6db3e9b9f5fee2cb5f3e1010ac6c783` | 11 passed; receipt verified |

The 150-PR replay uses 1,241 frozen-main test files after the two authorized
exclusions. It reuses gate selector/replay helpers frozen at `e3d91d46`,
with identical populations, quarantine, and duration fills in both arms.
The gate branch later moved to `9637d9bb` during the hosted-CI rework; this
experiment does not claim to evaluate that workflow revision.

| Rule | Selected files p50 | Selected files p90 | Estimated wall seconds p50 | Estimated wall seconds p90 |
| --- | ---: | ---: | ---: | ---: |
| Static | 205 | 1,043 | 181.919 | 721.820 |
| TIA | 232 | 1,241 | 200.370 | 771.545 |

These are the gate's nearest-rank percentiles and five-worker longest-first
file packing estimates, excluding pickup and setup. Node subsets retain the
whole-file cost. No replay row has an unknown cost. The full population costs
3,857.703 estimated serial seconds, or 771.545 packed wall seconds.

TIA is broader on this historical replay. There are 423 stale-source fallback
entries and no historical function-only match against the current source map.
Those fallbacks also retain dynamically observed consumers absent from the
static imports. A current-main map cannot prove coverage for older source
bytes. The replay therefore shows the cost of conservative historical fallback,
and establishes no selection speedup. A contemporaneous map per historical
base would be a different experiment.

Replay action
`271f86cf8416077a6bec7f2eff45a27442e9425be549a2f3ab55714bb3b178c4`
completed on sparky with its terminal record, CAS receipt, and stdout hash
verified. The history was captured through GitHub REST on 2026-10-31 at
20:47:45Z. Every merged commit object was included in the admitted snapshot.

The source-reader regression failed as intended in
`276f1e71bf3e99cac8523bab33a23de584328777508b5266761892ed1c90c04d`,
then the 20-contract suite passed in
`c489139d142056fe5f3d38fb1aa02a652ace3e7f9fe6b5729595eea2e3ab489a`.
The final suite also checks cross-runtime gzip headers and optional private
scratch ownership and cleanup. The private-root regression failed as intended
in `5b5f4dfa9389e840ef3ff383bd0f44e082a8c10333a19bf2f1095f868cb750f0`.
Its default-layout case passed, while the configured-root case failed because
the root was ignored. The red action has no receipt.

After the final short-scratch change, all 24 contracts passed five times on
each architecture, including two xdist runs each, with no pbtest basetemp
override. All ten terminal verdicts, CAS receipts, and result hashes were
verified. Representative xdist actions are
`b214c8ef55736b400d9d81e18642af6e4f1a76e7df679972aa6a07d34541ecb4`
on ARM and
`fe7171391d9a83b9e3a84c1c5803a42eb6013e610758d1729a23e4a7fabe5bc9`
on x86.

Five interleaved plain/recorded fast-tier pairs ran in one admitted PB
measurement action on sparky, Python 3.12.3. All ten arms passed 334 tests.
Median plain time was 8.811 seconds; median recorded time was 9.346 seconds,
an increase of 0.534 seconds or 6.07%. This includes pytest startup and source
hashing. Observed one-minute load ranged from 1.274 to 2.321. This bounds one
fast-tier experiment, not every suite or machine. Action
`521ebaa3a8ce6b8dd9d470bb3ed34e69b0cb5686899c06afb7d5fd4f1bd31d21`
has a verified terminal result, CAS receipt, and stdout hash.

The final overhead runner also completed ten arms of 334 passing tests with
short private scratch in
`f4ef6f890b65fa727d15d1fe412b5ea8a8cf7507fcafd882bb0edda8d341d862`.
Its receipt and result hash were verified. This was an ordinary admitted
functional check under batch load, not a new timing qualification. A redundant
measurement request was withdrawn from ready after `transition_busy` denials
on both Sparks. The qualified 6.07% figure above remains the earlier measured
sample. It predates the new resume and throw subscriptions and is not an
overhead measurement of the current recorder or scratch layout.

## Pre-review contract qualification

The final 25-contract suite passed five times on x86 and five times on ARM,
including two xdist runs per architecture. Every run used a distinct action key.
All ten terminal records report successful execution. CAS receipts and stdout
hashes were verified, and each report collected the same 25 contracts.

| Architecture | Repeat | Workers | Action key |
| --- | --- | ---: | --- |
| x86 | 0 | 2 | `7e70a9439a8245850a96867c0386fabc857cf6516da1d3551a8e4935ded5b309` |
| x86 | 1 | 2 | `7a360efa4b066128582215922308e1a72c0c8013361c2bbcc49fbc15151b21f8` |
| x86 | 2 | 1 | `2d78dbb5d12ac671a3a80bd1ef7ed9bc02a0bd8fb2d4457554a05d59834a8054` |
| x86 | 3 | 1 | `1e9e627ab828d142e6282c138bd14cffd015d8e4e0f2535972cc7ec8591a2efb` |
| x86 | 4 | 1 | `09893793625ee68c5f8cc8a889bcca59ded153e4881114ba94fb59b255250f5e` |
| ARM | 0 | 2 | `d0161f63d283a4fdb4187b67f601f5d4e3e4a1897767c9468ee5a29d7a866567` |
| ARM | 1 | 2 | `1508409f525109192c213098dc0d556a39bab15dbc4f88c811c42a5c6565d108` |
| ARM | 2 | 1 | `b39bb1eceafa8f2e776d7a8870824dfe2e298f72545e74e754060de1f324c526` |
| ARM | 3 | 1 | `fc7dbd9145b0c811c58e8cb732faff6ef64ac6023e1bdb0fc47d80859ca13c0b` |
| ARM | 4 | 1 | `f07bfc2a229e7bb943d3007e32fafe7db215cb25bc52286d9ae4d9e5468d5709` |

These runs qualified the pre-review 25-contract suite. The review corrections
below extend the suite to 33 contracts. Mutation acceptance remains open.

## Review corrections

The independent review at `78c477da` found four defects. Action
`d8332c2abac0d2e0dbdc0912f65ecef9545c7ceaf0be7d8e1dace4ff1b8e8e8d`
reproduced them with eight failing regressions and 25 passing prior contracts.
Its terminal stdout hash is
`81f7be7c3b114f557b6880f03a8a029fdef97d146313d1c9e055583a1146ed70`.
It has no receipt. Final green action
`67693c0ee86bbee7dc33836abd4dedbe75d245ba5fd4c3654c3e4f15b34ea0e5`
passed all 33 contracts under xdist at source `48cfc523`. The duplication guard
passed all eight tests in
`6e53eb2b3c0f0e38c3532e33ce489c0167fc48e7c16e260178584f71bb2e0432`;
the timing guard passed all 33 tests in
`67b91fa1f4ea9bd1ee7bf125f0144153e4331e04a85df82f622c00343f8efa10`.
All three ran on dl380g10. Their terminal records, receipts, and stdout hashes
are verified.

Initial green `ee9066fde4ebfa38d01e876e2d15aa0ccb3bf14488261356d75ecbd671f2c3e5`
passed 33 tests, but portable repeats exposed ancestor pytest root discovery in
fixture repositories. Actions
`d082b3ff3400433c99b50fc9ed78c9b5ec74082e94e7ae240e086a82a7d6770d` and
`856b7ad938d7588f3bf8f2c43fde4262ef710dd36c17e7d606ee5afe745e1284`
each failed the exact collected-node assertion on ARM. Evaluation now seals
`--rootdir=<evaluated checkout>` so node IDs describe that checkout on every host.
The duplication guard also rejected a second JSON serialization recipe in
`03a221e4b546bafb03b47ce5b1668e2b42a85d4fe921fd44154a7e8d322f5678`.
The completion plugin now reuses `tia_bytes.sorted_json`. These failures are
retained and are excluded from final qualification.

Final portable repeat qualification at source `48cfc523` passed all 33 contracts
in five submissions, including three xdist runs. All five landed on dl380g10,
x86 Python 3.14. No five-run ARM qualification of the corrected source is
claimed; earlier ARM root-discovery failures remain recorded above. Portable
placement was retained as requested.

| Run | Workers | Action key |
| --- | ---: | --- |
| Initial final source | 2 | `67693c0ee86bbee7dc33836abd4dedbe75d245ba5fd4c3654c3e4f15b34ea0e5` |
| Repeat 0 | 2 | `6e900aad895b59288f179154693f2ea92bd35ac20147abf9ad780731fb4b1007` |
| Repeat 1 | 2 | `21523052bb7067016677d7e9bde16fc045a9cffed5548aab19f4bbbed30232a3` |
| Repeat 2 | 1 | `0301a30f4ac9c2cbe311095b0ee8a905b73db27389ae78ef7f075f13b77ec0dc` |
| Repeat 3 | 1 | `f58f2c8258b0470ccb927868736ae8dd869d66e639b62d5902413927c9cd7067` |

All five complete shard verdicts, terminal records, CAS receipts, and stdout
hashes are verified. No tests were skipped.

- Default selection and replay commands use the adjacent gate's actual
  `load_gate_json`, `replay_selection`, and `summarize_replay` APIs. Integration
  contracts invoke both default commands without substituting gate modules.
- Evaluation arms require explicit collection and execution completion evidence.
  A real interrupted pytest run with partial JUnit is invalid. A real collection
  failure remains separate kill evidence, outside the valid comparison count.
- Generator attribution covers normal and exception-driven resumptions across
  test boundaries. The deterministic contract advances one shared generator
  under three node IDs and verifies attribution to each.
- Population exclusions use pytest's prefix matching. Both `[False]` and
  `[True]` cases of the excluded completion test are removed during recording.

The 53 existing comparisons predate this completion protocol. All 159 arm
return codes were rechecked and are 0 or 1; none used exit 2. They remain
historical evidence, not runs of the corrected recorder or a hosted-runtime
map refresh. The large mutation campaign remains held until the lead releases it.
The static-selection miss found during evaluation has its own fix in #2474,
tracked by #2472. Its broader selection must be assessed separately from the
frozen static rule used by the historical TIA evaluation. Its verified replay
of the same 150 PRs has selected-file p50/p90 207/1,043 before and 247/1,165 after,
using a 1,249-file current population. That replay is not a new TIA comparison.
Action `1f767987e1c127889d2eb5d64a1a02c8446862a0ce83146295a1cf8749f76635`
has a verified terminal, receipt, and stdout hash. Missing duration inventory
entries leave wall-time costs unknown. A selected GPU-policy witness passes on
baseline and kills `return not classes` in action
`f80db52a19b7b28259fa3e4d95b77e8bbfacb1d58d747072223435f62aa1c476`.
This bounded diagnostic executes the selected killing node, not the complete
1,112-file selection. It does not resume the held mutation campaign.

## Mutation acceptance remains open

Mutation acceptance is still in progress. The collection at 01:51 UTC contains 53
receipt-backed historical comparisons: static killed 48, TIA killed 49, and 0 were
static-killed/TIA-survived misses. They have no completion ledgers, so they do not
count toward acceptance. The required 100 valid comparisons must all be run under
the completion protocol with the corrected recorder; none have been run yet.
The recorder's overhead must also be measured again before shadow mode, because
the 6.07% sample predates the resume and throw subscriptions.

The quota-affected campaign was superseded. The invalid relative-layout Spark
resubmission published 113 of its frozen 120 rows before withdrawal. It ended with 101 withdrawn rows and 12 failed actions. Five
completed baselines failed the three scratch-layout nodes listed above. Six
baselines reached the 1,000-second arm bound, and one failed while cleaning a
still-changing private temp directory. No row reached a valid paired verdict.
That discarded run provides no kill or miss measurement.

The evaluator now accepts an explicit `PB_TIA_SCRATCH_ROOT` for a qualified
short scratch directory outside the checkout. It creates a unique private
directory there per arm, uses it for pytest and `TMPDIR`, and removes only
that owned directory. An unset variable uses `/tmp`. The lead authorized this short private scratch
lane on the Sparks. The corrected campaign uses 1,800-second arm bounds and
a 6,000-second action bound, while retaining one CPU and 4 GiB per row.
Failing-arm stdout is compressed into `tia-arm-log: BASE64` records in CAS
stdout before the result marker, so cleanup does not discard the diagnostics.
Qualified Spark action
`103509ebc8f37a6d9735ab1a0481db27ccb39775af906912afa0f5572d2fc335`
passed all three diagnostic nodes on sparklina with a verified CAS receipt and
result hash. The final 24-contract suite passed under xdist in
`f49fc0c4fcb33902d58bb5bfe62cd5303565e26b4b994caa94b6c32a25be906f`,
also with a verified receipt. The default-short-scratch regression first failed
in `fd89b4ca11c73f46407066d5a2fe11cbc5a88de71523a2031ac2f300485e6886`,
with no receipt.

A cleanup failure in the discarded run showed that killing only the pytest
parent can leave descendants writing to private scratch. The final launcher
starts an owned process group, kills that group on timeout, and drains output
before cleanup. Its deterministic process model regression first failed in
`f918f6e77656b57d423bf9a0e66166a776de10a9cdd1bf717b947ad977a4fea3`
with `assert True is False`: the descendant remained alive. The final
25-contract suite passed under xdist in
`cd58a46deb2f5123396a19e189a3827b1c47b987af2da2cab686b4631d07037d`,
with a verified receipt. Already published mutation rows retain their frozen
evaluator. This launcher change affects timeout cleanup, not selection or
successful pytest verdicts; interrupted rows remain invalid.

Final launcher validation passed all three diagnostic nodes in portable
action `e19c1fd271102b504c11d9aad493244966d436b557ca62f629f8d57a510dbf61`,
with a verified receipt and result hash. The queued Spark copy
`0874ba3b88a676a7901b2897fde0b716bc53e87cc68ed1eeb04fc97f09ec8ccb`
was withdrawn only after a fresh ready-state check. Its published withdrawal
record confirms `state_before: ready`; no running action was withdrawn.
The final duplication guard action
`a0433a0718426ea0bf63a3e0facf699dc8bd4a9a3f2e331553233ec292e4898d`
reported seven passes and only the authorized same-name baseline node, with
the same two existing names. It has no receipt. Its verified terminal stdout
hash is `3c0ef2049c61961a599e34392d1518abc398a15628a48fde4b60641094f06cd5`.

The three scratch-layout tests remain in both arms. They have not been
reclassified as authorized baseline failures.

The first short-scratch timeout-cleanup failure was action
`878ce56a927d965d0bab7188ef1a107a4ee594d453ad2976fb1583fff95551c3`
on sparklina, with no receipt. It ended after 1,844 seconds in baseline cleanup
with `Directory not empty: fleet`. Its verified terminal stdout hash is
`d014c59b6846f96d3cbf904ff7fbd10b7c5bb32f4af456936738e9cc5c042da4`.
Action `eca7f7c2f9e89e3210d0066acfe72e688fc8fd8498d1e0c65fd67d434f36a83c`
also ran on sparklina and timed out after 1,800 seconds near the end of its
237-file baseline union. Neither action counts as a mutation kill or miss.

A third original action,
`abc21c113899fa5a229818769930854ed6e3983b65572bc01b587d22e788989d`
on sparklina, exceeded the TIA process bound after pytest reported 8 failures,
5,028 passes, and one skip in 1,798 seconds. Static reported the same eight
failing nodes and completed. This `pbresident.py::submit_copies` mutation is
excluded from valid comparisons because the TIA process returned 124, even
though both arms observed the same failures. Its verified terminal stdout hash
is `e8aa20f22d9a15322728b010b358ef34d23e476069b0dfe6bb565b55801ad4ac`; it has no receipt.

The final-launcher retry manifest contains the same 87 mutations withdrawn
from ready, plus these two terminal failures. Every withdrawal was preceded
by a fresh exact-key read and confirmed `state_before: ready`. One candidate
had become claimed and was skipped. No running action was withdrawn.
Retries retain one CPU, 4 GiB, priority -10, and `gb10`; an admitted wrapper
sets the arm bound to 2,400 seconds and the action bound to 7,800 seconds.
The retry manifest SHA-256 is
`af6e011ebb2dc616ba1894506f9b5b24fcf3268617a387a2d3367555997f166b`.

A portable verbose diagnostic,
`d154540d4f81e2ab690668b546d4fc0dfb6839d4d060d19c30610b3a080fe72e`,
used the final launcher on dl380g10 with a deliberate 300-second bound. Tests
were still progressing at that cutoff. The launcher retained partial output
and cleaned its private scratch. This is timeout-path evidence, not a passing
baseline; it has no receipt. Its verified terminal stdout hash is
`00c3ca6dcec8074c7699fd4d6562aba75f106ead99113a4c3d7a87f281052296`.


The short-scratch campaign freezes 120 mutations, 60 in each requested tree,
across 18 modules. It contains 55 return flips, 63 condition negations, and
two arithmetic flips. There are no comparison-only flips in this sample.
Sampling chooses covered functions deterministically by static cost, path, and
qualified name, interleaving the two trees. Results therefore describe this
covered, lower-cost sample rather than a random sample of PB functions.
The frozen manifest SHA-256 is
`f5f47126d05663bd44f69347738c7933ec901c0a5714dd491ef3e98da9c60418`.

A completed example demonstrates a static dependency gap. Action
`1498d321b76cc9fe4abbe4096915cb5222dce336d3e3028b6d1839359c0fddd1`
mutated `_gpu_submission_policy.py::_eligible_gpu_classes` from `return classes`
to `return not classes`. Its clean baseline passed 4,603 tests. Static selected
193 files and passed all 4,469 tests against the mutant. TIA retained those
files plus 79 recorded nodes in `test_gpu_submission_eligibility_2036.py`,
`test_pbcampaign.py`, and `test_pbrun_gpu_contract.py`. Of those additional nodes, 78
failed through the mutated return value; 4,470 tests passed in that arm. The recorded
function reason selected these nodes; they were absent from the static plan.
This is a TIA-only kill, not a static-killed/TIA-survived miss. Its successful
evaluation action has a verified terminal record, receipt, and result hash.

The execution checkout `f994c971` contains the frozen evaluator and unchanged
gate helpers under `evaluation_support/ci/`; `1f1ee74e` adds the final timeout
launcher for subsequent diagnostics and retries. Neither checkout changes the
frozen-main production sources before applying its declared mutation.

## Queue pause requested by the readiness lead

On 2026-11-01 at 00:35:38 UTC, the lead requested a 90-minute pause so an
urgent live merge-gate fix could run. The published withdrawal procedure
removed 66 mutation rows. Each was freshly checked as READY with zero attempts,
and each withdrawal record confirms `state_before: ready`. Two candidates
became claimed before their fresh check and were left running:

- `521acdb7248b2de9127d4ee6ede37f42856c8e577b9df372b7a65627652614be`
- `ac985f4a1124a4076511909e561b8c501782a68208072a3a1c638839b219e120`

The lead extended the hold at 01:55 UTC until explicitly lifted because TL3
Kani proofs are memory-bound. The earlier 02:05:38 UTC boundary no longer
authorizes resumption, including after completion notifications. The resubmission manifest
retains the same frozen arms, inputs, resources, scratch convention, and
bounds. The newline-delimited withdrawn-key ledger has SHA-256
`05dd40125bbeadc6a088608581a8399a72830a528c1e84a5b1812b26948be150`.
At the pause, 30 valid comparisons supplied 90 completed arms. Two more arms
completed in an excluded timeout row. Running arms are not counted as complete.

By 01:51 UTC, all 23 V5 rows left running had completed successfully. Together
with V4, these provide 53 valid comparisons and 159 completed valid arms.
The 66 withdrawn keys remain unchanged. Each V5 controller part returned 1
because its manifest includes withdrawn work. This controller status does not
turn successful action results into failures. There are no new failed actions.
TIA selected the same number of files as static in 51 comparisons and more
files in two. The four mutations both selections survived are
`bench_claim_pass.build_and_poll`, `updater_proof.last_proof`,
`updater_proof.tool_sha256`, and `admission_shared_io.git_head`.

The exact keys withdrawn for this pause are below. Ordinals refer to the
frozen 120-mutation sample.

| Ordinal | Withdrawn action key |
| ---: | --- |
| 42 | `3899c51e4b333809b33681795249e1f3cb6bfc084912114aec4565dc798a435e` |
| 47 | `de578ec89134e562a35e71dabcd0a230d09113c31e43ec02acd4baf04ab4c44b` |
| 49 | `2cf2414876f96570897f27da96f168c7cce174842c604fffae77ed49f2a60d98` |
| 50 | `cae37df6410e6451014814e2aa9b81291aff33a67e581c77ade79909d4680ff2` |
| 51 | `b2726135f8f0290e8593249d4629bd3ad7f13ee0f662cffb8a396d433f5cb26c` |
| 52 | `a412ee2ea388bb852611b76a088238631e4a392272ceda2f870bd6a19664e2ab` |
| 55 | `04e27157b309b74d7bec31e06537509b90c3787098f1d55a7fee1a2d2312edbc` |
| 56 | `0334136aad2bd7fc120a155e4fd5f8a9e49d31b08ae94c9cfb7fed60c7563f67` |
| 58 | `ac7d561683ab331620731b508be47db178684315f4015dc6bdc43dcc230b92cc` |
| 59 | `150d9bdf697e82f2686c3fa53f83c9f4e7e4c4652e25c9a45728dbd69a0b4142` |
| 60 | `f48ae69dd662893d937fbe3074d14b271bdfeabde60fe64a1e1b4e49bda4276a` |
| 63 | `0403d994e90f1125354b3252f583bc7a9b623bd967288ba4dd79ea0672517f8e` |
| 65 | `d89f50743d92edef1e71f44d0473b99d60afa87b86aa10358d1d90bc9d43adcd` |
| 67 | `d35b638df290f3788375c4ddbed5892af573427dc9a22ba582cf9feaccd5597e` |
| 68 | `23a285017f84b076f7337f40d8c88e68724b27c90b108825e899d0fb9efdc3b3` |
| 69 | `8377759c567dd0b171a49d9e134b15eecdadc3f8e54bba9b44a86bbc34088325` |
| 70 | `bb8a066442c7e91a2c6141f8f5f00c8c6ec583b26a09ade8180511085263e8e4` |
| 71 | `6ab36c686b6033533b15a2f34368effe222b6f21073188b9dc89868ca2c5c275` |
| 72 | `10422e6250e3c939b3c1e6779a3038134afc511e7bad75eb7627bc92212f05de` |
| 73 | `49c6b4335dd005ffd003042f8eb1d3c2ae0be2e24aa552fe746f71e3ec97996f` |
| 74 | `1dcdcc8f597c484d7c7b3d99f978244d5719e7eee5758c3f3131dfcd316fd7ce` |
| 75 | `ac7ee9989e96fe9af1bb9b21814a64fe2ab63ced8456c29ab4a5a9ccced38d46` |
| 76 | `21a7ec1fefe4e519b7f6be2e9f54be1314179f7124cc6080ee67d09ec892e95e` |
| 77 | `45bbd32c89254c27649a63f66a1cacbcb1ef0046c942ef996692313f357613a3` |
| 78 | `d22716315eca5323eb7b22b7f3f38144c7ac7256c9f49a6170a0538dc6ee2b4b` |
| 79 | `7cddc634f1e958b1c89705b19c7f3b6b34a5b8003215a4e3785c4f1024beea06` |
| 80 | `43c27cf55e4c5eed86f4d184f23b4a4c88166a5223ddc12e19d8823d0de767df` |
| 81 | `36c652a77989d862240ab48a8fec8f969565c55f62ed3d079d3079404d9c01f9` |
| 82 | `c5453b64a7663c1ce7c640ad16d9d570047a5721fb18958b783696a26ca70534` |
| 83 | `dd1227adf693d426799597108817b9e4308f07620b770245174ad19c82db0e80` |
| 84 | `fa9e501144818e1f340f53a4f6d7c53b4e2283771a2370d144b9eef97801b4ab` |
| 85 | `9f6a719fcd6e6928136f8dfc7473094d7fae36c313717db7f634aff977e239c9` |
| 86 | `f8b1e39b194bc3c5e9b107a77e044fa29ee9e9fd81f09726499cdda3e53eb10f` |
| 87 | `b414c5382c3f8b8792cc6241d31d35e491ae1feeeb39fd34634f6d7ffdad336b` |
| 88 | `e01197406a6b5ac60c167a6f878cdd38e0d57bf438d47250a02c1005a7532a8c` |
| 89 | `b7634fa6543c9ede77b2df353e13d8e91dc14c2bc616ae3555c4264b876439d0` |
| 90 | `8c81b767baec42be5912ac47cf6fa8790ce09fc9e1db1ef5aa662ee43678b3e7` |
| 91 | `1771bde57f2cba2c3a2316af250a3114931c0c5d1fe639c55e3156286fc654b2` |
| 92 | `357820dce1c3ed23e7229e86db2a89c75e43f5a1654b5c7caae0d592e2f6329c` |
| 93 | `ed59ddc753433b08f85a4a235fea47813ee85b470b0c44a810d21aba335b98a8` |
| 94 | `181be05ce32797a37a006505315a26e9aaa93336e862e7f712a52896473e192d` |
| 95 | `ce35a3b6c4e8c6f63e6ab2da6ebd7e3c1ecbf3e0eef4cd161733385242da5f7a` |
| 96 | `c989a76d3331dc586f494fad48c24fd173f09a3a7aefe441c5bdafaa4e9d94a9` |
| 97 | `51a9b3a69ff2fcc8d432f488276173f23a6b892a64833e05295cf5d5f0161024` |
| 98 | `c9f18cc615a0bd198c8282f5d51399f24ae2f3f4285dbf1ff8f2fd887314dbff` |
| 99 | `2c88bfcb5adf4cdf3b54415f63fb538ce59010e74e53bfe732460ef7365bc02b` |
| 100 | `996595294f05447b041a48fec074ab65e6bcc64a9f135f270579e1a39c9f72a8` |
| 101 | `729f511d612027b4752943c65b882bace5b25ce0e8e342edabd870ec7ed0d32e` |
| 102 | `7c08ec38b1241c07c4d06f85e4a014aa55fe2d0652a48565ee2b87a38382ddd9` |
| 103 | `247597751e1c32b07fa2cdd1469f477cf5cdf98a7d30568ddc83ca385af51450` |
| 104 | `1eebe628a2f18fec2de51f2b69475cae871c1ffe580985adc56b03c14fb7c6bc` |
| 105 | `4adf9551f7029ce96fb2f153083f42457c4d5c047f1f8d403848d86898698dd8` |
| 106 | `8de720482e824d3ee3e20a17787636b010afaaf8cce86733c850016f4b38bc52` |
| 107 | `f6972778fea2e96f594379bbf614eef98d8a07cd4d4d8369e3e8972490059d32` |
| 108 | `3285e8086571015ce9ae6cd21c06b112c706334f89dc811f1ac8e87490361905` |
| 109 | `6a220f84dd76fab870623f37f28ed7ead98e70b60a2ede763327e8591387b6d8` |
| 110 | `7643104bbe6a3c21557f5a010e574d0a820cd47c6774f73821663d7d8f47bddb` |
| 111 | `5e75ab16719b4ae7f951b9ce0686a9af5f7f2d4f1f62bd5e30ac659ec41d185b` |
| 112 | `e4f39fe65dd1347c63a7bf2ae19c54a85cf9008a2536bd3aed3067c321a877cc` |
| 113 | `c0040d6b2ffd0ce0c58e81d72248b8a10bbc0a0cdc1570c08b3954daaf80b021` |
| 114 | `dde39533ffd4c34c8e3305d46ea87889ff636a5e9d350900145fc5af706f776d` |
| 115 | `315e866aea324e8427549b0ac454cff436b3b9fddc6e6277271f1727229c9bed` |
| 116 | `d09d02b717556ad759daad530c18c634becbe7ac840a7739c469c0d05b776d95` |
| 117 | `41185480eef42d0692d842df7831aaa6fe30b66f56f0b67b4fae97fff4a35534` |
| 118 | `df5c0dbaccd2450c6f9168d292df5906ea083c583c34b2b2801082ef9ecbbac2` |
| 119 | `daa17cd39519ff6590791995a12f4b2dc82171d9dfefbe3b7840af151f4a53fb` |

## Coverage limits and integration recommendation

Environment inheritance is an explicit boundary. Python launched with `-I`,
`-S`, a replaced environment, or a replaced `PYTHONPATH` can bypass the bootstrap.
A process killed before its exit hook can lose its records. Native code is not
traced. Background thread calls are attributed to the currently active pytest
node, without tracking the test that originally started that thread. Coverage
describes executed branches under the measured Python and architecture; it
does not prove unexecuted branches or configurations.
Unchanged bytes in a changed module do not prove unchanged coverage after
tests, fixtures, other imports, or configuration evolve. Refresh the map with
those changes. Accept the review fixes, 100 valid comparisons, and a map refreshed
on hosted runtimes before starting full-suite shadow comparisons.
Fixture dependency sharing covers calls observed in the parent during setup.
Child-only setup calls are recorded under the parent test node, but are not
merged into the cached fixture dependencies of every later consumer. Finalizer
calls remain associated with the pytest teardown node under which they execute.
These arrangements need further recorder work and full-suite shadow evidence
before TIA can replace complete queue tests.

Refresh on the CI runtime and collect the union of the configurations the gate
supports. Keep static fallback for missing functions and hash mismatches. Run
full population shadow comparisons before using TIA as a required selection
gate. The mutation sample covers the stated operators and selected cheap
modules; it does not establish universal completeness.

The readiness lead explicitly excluded these files from development map builds
and mutation evaluation after exit-78 refusals on dl380g10:

- `tests/test_record_compatibility.py`
- `tests/test_current_receipt_compat_composition.py`

Their map metadata records the reason, "bubblewrap cannot mount /proc in
admitted PrismaBuild actions; exit 78". The selector still picks either file
when the static import or literal-path rule selects it. Evaluation excludes
them from both arms. The gate agent owns their classification on GitHub-hosted
runners; this change does not alter the gate's population files.

The observed exit-78 actions were both on dl380g10. No observation establishes
that either Spark has the same refusal:

| File | Failed action, no CAS receipt |
| --- | --- |
| `tests/test_record_compatibility.py` | `2ce7878d2a398c8b98e2bb0f061bcd8074c86ed1fd3fbe901161f6f192dce42e` |
| `tests/test_current_receipt_compat_composition.py` | `11a75c845d5176f964298e6230562e6471ba3c604f6304a3b2ef36982970c040` |

The unmapped-file regression first failed in action
`a37bd9cc20b25ff70d5c680595621ddb5d7cbc8635871d4a0a72893a4f35a9b3`,
then all 18 contracts passed in
`6d2d621a0ea8a4f669f40d4c03185b795ba22036aac9d4a8791833b8b68132ab`,
with a verified receipt. The later catalog-reuse regression failed in
`96ea5cc90ccd406296d186361fd6813b12a67fa7905ed2c50c251300ea0ee778`
and the corrected 19-contract suite passed under xdist in
`8a156a86de431a743067719e5ef9af4d8497fafae79c86553aceb081f7ea10b8`.
