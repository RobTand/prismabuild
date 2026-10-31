# Function-level test selection

PB's TIA tools record calls by pytest node ID, compare Python ASTs, and emit
pytest targets with a reason for each selected file. The tools do not change
CI workflows. PB's CI uses GitHub Actions on GitHub-hosted runners, with Mergify
as the merge queue. The lead owns that integration in #2433.

## Recorder and map

`tools/ci/tia_record.py` is a stdlib-only pytest plugin for Python 3.12 or
later. It reserves an unused `sys.monitoring` tool ID and listens for
`PY_START`. Each callback returns `DISABLE`. Test boundaries and fixture
boundaries call `restart_events()`. The recorder keys functions by repository
path and `co_qualname`, including nested functions and methods.

Only calls in `src/` and `tools/` are recorded. Module code is excluded.
The plugin hashes source files before tests start, so callbacks do no filesystem
I/O while tests run. Shared fixtures carry their recorded dependencies to every
consumer, including tests that reuse a cached session fixture.

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
| `PB_TIA_EMIT=1` | Emit gzip/base64 records to CAS-captured stdout |

`tools/ci/tia_map.py` merges records into `prismabuild.tia_map.v1`. The gzip
JSON contains a sorted function table, integer function IDs per test, file
SHA-256 hashes, the source commit, and observed Python versions. Sorted tables,
compact JSON, and a zero gzip timestamp make repeated merges byte-identical.
Mixed source commits or conflicting file hashes are refused.
Functions executed outside test boundaries are recorded separately, without
attributing module execution to tests. A change to one of those functions
takes module fallback, even when other tests also called it directly.

The nightly refresh must run the complete gate population with both
`tools.ci.tia_record` and `tools.ci.tia_population` in `PYTEST_PLUGINS`.
The second plugin applies the gate's unchanged node quarantine. PB's `pbtest`
partitions development map runs, with at least 4 GiB per shard. CI's nightly
refresh runs on GitHub Actions and does not submit through PB.

A refresh is usable only after every shard has a complete passing test verdict
and a verified CAS receipt for development evidence, or the equivalent complete
CI artifact. `tia_evaluate.py extract` refuses incomplete development reports.
`extract-cas` verifies result blob hashes and merges records inside an admitted
action. Missing test files remain whole-file selections until a complete refresh
records them.

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
| Test helper | Gate's transitive helper rules |
| Selection infrastructure or test configuration | Complete population |
| Test file absent from the map | Complete test file |

Hashes are compared with the changed file's **base** content. Comparing with
its changed head would invalidate every useful function match. The source
commit is provenance; a map from another commit is usable only where the base
file's bytes match its recorded hash.

The selector imports `gate_select.py` for population, fast tier, quarantine,
static dependencies, and estimates. `--gate-dir` can name an unchanged copy
from the gate branch while that PR is separate. `tia_evaluate.py replay`
imports `gate_replay.py` for the static comparison and percentile calculation.
It does not maintain a second static selector or replay implementation.

The estimate remains the gate's whole-file cost, even when TIA chooses a subset
of nodes. It is a conservative file-cost estimate, not a measured node runtime.
Missing durations remain unknown.

## Evaluation status

The development evidence and outstanding acceptance work are recorded below.
No CI job relies on TIA yet. Keep the queue's full population and collect shadow
plans until the population map, mutation comparison, and overhead results are
accepted.

The repeatable evaluation tool has these commands:

- `extract` and `extract-cas` merge receipt-backed population records.
- `replay` compares the latest 150 merged PRs against the same current
  population as `gate_replay.py`. Historical base hashes must match; a newer
  map does not silently claim historical coverage.
- `campaign` emits a `pbcampaign.py` manifest with one mutation per row.
  It chooses deterministic, covered functions in `src/prismabuild/` and
  `tools/fleet/`, interleaves both groups, and limits static estimated serial
  cost to 30 seconds per row. This biases the sample toward cheap modules.
- `mutation` runs a clean baseline, then the TIA targets and full static
  selection against the same mutant. It restores source bytes and records
  failures, collection errors, counts, skips, and incomplete runs separately.
  A non-green baseline is invalid evidence. It never counts a timeout as a kill.
- `overhead` runs five interleaved plain/recorded pairs of the fast tier in one
  PB measurement action. It records Python, observed load, exit codes, and
  timings. Tests assert no timing threshold.

All development commands that execute tests, build maps, replay history, or
measure performance run through the published PB clients at priority -10 from
clean committed detached worktrees. Mutation rows request one CPU and 4 GiB,
bound native threads, and let PB choose any eligible worker.

Initial contract evidence at `47d56296`:

| Contract suite | PB action | Outcome |
| --- | --- | --- |
| Missing implementation | `703bf93248f38dff934b0b2fddf8c0ab1a6fc5b311c154eef012b1f177492a35` | 11 intended failures; no receipt |
| First implementation | `1b30d4f9ea896dff6c55ba60b4f56113e6db3e9b9f5fee2cb5f3e1010ac6c783` | 11 passed; receipt verified |

## Coverage limits and integration recommendation

Environment inheritance is an explicit boundary. Python launched with `-I`,
`-S`, a replaced environment, or a replaced `PYTHONPATH` can bypass the bootstrap.
A process killed before its exit hook can lose its records. Native code is not
traced. Coverage describes executed branches under the measured Python and
architecture; it does not prove unexecuted branches or configurations.

Refresh on the CI runtime and collect the union of the configurations the gate
supports. Keep static fallback for missing functions and hash mismatches. Run
full population shadow comparisons before using TIA as a required selection
gate. The mutation sample covers the stated operators and selected cheap
modules; it does not establish universal completeness.
