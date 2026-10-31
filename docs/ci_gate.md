# Hosted Actions merge gate

Ordinary PRs run `pr-gate` on GitHub-hosted `ubuntu-latest`, selecting tests by
evidence. Mergify batch drafts run the full eligible population when the head
branch starts `mergify/merge-queue/`, the PR author is login `mergify[bot]` with
id 37929162, and the head repository is this repository. A same-repository PR
with the `gate-canary` label also runs the full gate, without duplicating the
single-job PR selection run. No CI job uses the fleet.
Mergify requires `pr-gate` to queue and the final job `gate` to merge.

The full plan records a shadow selection while coverage stays full. The initial
matrix has ten shards, alternating `ubuntu-latest` and `ubuntu-24.04-arm`.
Workflow reruns swap the architectures by shard index, so qualification exercises
each file on both architectures.
Each shard uses four pytest-xdist workers within inherited CPU affinity.
Actions are pinned by commit SHA. Setup uses Python 3.14 and pip caching with
pytest 9.1.1, pytest-xdist 3.8.0, PyYAML 6.0.3 and setuptools 80.9.0.
Hosted setup installs bubblewrap and creates the real local scratch directory
declared in the fleet roster. It exports that directory as TMPDIR. Proof capture
runs without setup-python's ambient LD_* loader overrides; the interpreter must
also pass its version check without those overrides. Native threads are bounded to
one and each test has the integration tier's 300-second failure bound. A test over 60 seconds is reported as a cost issue for #2328, not a speed
verdict or an exclusion. Permissions are read only; checkout credentials are not persisted and no secrets are configured.

## Plans and partitioning

The selector includes FAST, changed tests, direct production importers, literal
path references and the test-helper import closure. It resolves named directory
bindings, __file__ parents, test-relative paths, Path constructors, joinpath and
os.path joins. Computed fixture names conservatively match their containing
directory; an unresolved base may retain a literal suffix. Changing conftest,
pyproject, verify, lane manifests or the gate's code/config selects the full
population. Documentation alone does not.

The shared partitioner is also used by the nightly full-suite workflow. It sorts
by descending duration, then path, assigning each file to the least-loaded shard
with the lowest index breaking ties. Unknown files cost the nearest-rank p90 of
known costs; empty or all-zero inventories use one second. Unknown file costs
remain identifiable. Every file is retained exactly once, and the interface
retains empty shards when there are fewer files than shards.

```sh
python tools/ci/gate_select.py --base "$BASE_SHA" --head "$HEAD_SHA" --mode full \
  --output plan.json --files-output files.txt
python tools/ci/gate_partition.py matrix --files-from files.txt \
  --durations tools/ci/gate_durations.json --shards 10 \
  --runners ubuntu-latest,ubuntu-24.04-arm --output matrix.json
python tools/ci/gate_partition.py shard --files-from files.txt \
  --durations tools/ci/gate_durations.json --shards 10 --index 0 \
  --output-files assigned.txt
python tools/ci/gate_run.py --plan plan.json --files-from assigned.txt --workers 4 \
  --result result.json --junitxml junit.xml
```

Each shard recomputes its assignment from the same population, duration table
and shard count. No test-file list passes through matrix entries. Coverage is
never truncated to meet a time budget. The initial count and timeout estimates
must be assessed against live hosted-runner timings, including pickup/setup.

## Verdicts and retries

Exit 0 means pass, 1 means test failure, and 75 means no verdict. Logs distinguish
`GATE_PASS`, `GATE_TEST_FAILURE` and `NO_VERDICT`. Setup/planning exceptions are
infrastructure failures. A shard's first attempt uploads its result and xunit1
JUnit with file attributes. The retry planner chooses only missing or invalid
verdicts, including a lost runner or failed setup. A test failure is never retried.
A separate job retries each chosen shard once, so retry can survive loss of the
first runner. The final `gate` job fails on any test failure or a missing verdict
after retry. A valid first verdict cannot be overwritten by a later artifact.

Artifacts are separated by workflow attempt and shard/attempt index to avoid
stale evidence on reruns. Plans include base/head SHAs, population version,
quarantine and duration provenance. Job summaries report selection/assignment
and verdicts. JUnit and final effective results remain available on failures.

## Shadow selection

Each full run archives the shadow selection and effective final shard records.
Compare selection against full-run failures with:

```sh
python tools/ci/gate_shadow.py --plan plan.json --results effective/*/result.json \
  --junitxml effective/*/junit.xml --output shadow-accuracy.json
```

The audit returns 0 for no missed observed failure, 1 for a missed failure, or
75 for incomplete evidence. It requires complete, unique assignments, actual
JUnit file coverage and matching population/base/head identities. Fully skipped
or empty files can make evidence inconclusive. Shadow accuracy does not reduce
the full gate population, and one successful audit proves only that run.

## Population and quarantine

The gate excludes root, GPU and dedicated two-CPU lanes declared by
`tools/verify.py` and the proof/receipt manifests. It excludes whole files in
`gate_quarantine.json` and deselects its node-prefix entries. Every quarantine
entry needs an open owning issue, a specific reason and evidence. Readiness
tracker #2428 is not an owner.

`environment_lanes` records capability requirements separately from quarantine.
It supports complete files and precise node prefixes, retaining portable tests
in mixed files. The hosted trial identified published-runtime dependencies
under #1182 and broker/worker fixture requirements under #2442. These controls
stay in their capability lane; they are not classified as flaky. A three-file
PrismaBuild comparison passed all 109 tests with verified CAS receipts, while
the hosted trial failed representative cleanup/scratch controls. The gate does
not install a fleet supervisor or broker, fabricate a shared runtime, or relax
production containment to make those controls pass.

The first hosted arm64 trial found no bubblewrap executable for
`test_record_compatibility.py` and `test_current_receipt_compat_composition.py`.
Qualification after installing bubblewrap must establish their behavior on both
hosted architectures before either admission or a separate environment lane. Historical entries remain provisional under
#2434; archival replay stays with #1997. Filesystem-floor skip/pass variation
is not called flaky.

Remove an entry after root repair or five fresh passing repeats on each
architecture in the CI environment, as the owning issue requires. Quarantine
contains a failure; it does not repair it. Inventory durations are summed
per-test medians at 22e9d752. New files retain unknown inventory costs, and
partly quarantined files retain conservative whole-file estimates. Replay and
pool.py AST diagnostics are in `tools/ci/gate_replay.py`; function-level selection
remains follow-up work.

## Path-loading survey

```sh
python tools/ci/gate_paths.py --root . --output path-survey.json
```

Counts overlap and include temporary paths and generated fixture output; they
do not prove dependency completeness. Loader arguments contribute static path
evidence. with_suffix/glob operations retain conservative directory matches.
Paths supplied only by runtime arguments, environment or arbitrary helper return
values can remain unresolved without a static repository directory or suffix.
Queue coverage stays full while real-batch shadow evidence accumulates.
