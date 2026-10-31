# Hosted Actions merge gate

Ordinary PRs run `pr-gate` on GitHub-hosted `ubuntu-24.04`, selecting tests by
evidence. Mergify batch drafts run the full eligible population when the head
branch starts `mergify/merge-queue/`, the PR author is login `mergify[bot]` with
id 37929162, and the head repository is this repository. A same-repository PR
with the `gate-canary` label also runs the full gate, without duplicating the
single-job PR selection run. No CI job uses the fleet.
Mergify requires `pr-gate` to queue and the final job `gate` to merge.

The full plan records a shadow selection while coverage stays full. The full
matrix defaults to sixteen shards, alternating `ubuntu-24.04` and `ubuntu-24.04-arm`.
Workflow reruns swap the architectures by shard index, so qualification exercises
each file on both architectures.
`GATE_RUNNERS` is the workflow's single list of test destinations. The small
label-resolution job bootstraps on hosted x64; test jobs use its outputs,
and `matrix --runners` receives that same list. Changing test destinations
requires one edit to `GATE_RUNNERS`.
`GATE_SHARDS` is the single workflow setting for the full shard count. The
lead sizes Mergify parallel checks so shard count times concurrent batches
stays near the account's 20 hosted Linux jobs.
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
  --durations tools/ci/gate_durations.json --shards 16 \
  --runners ubuntu-24.04,ubuntu-24.04-arm --output matrix.json
python tools/ci/gate_partition.py shard --files-from files.txt \
  --durations tools/ci/gate_durations.json --shards 16 --index 0 \
  --output-files assigned.txt
python tools/ci/gate_run.py --plan plan.json --files-from assigned.txt --workers 4 \
  --result result.json --junitxml junit.xml
```

Each shard recomputes its assignment from the same population, duration table
and shard count. No test-file list passes through matrix entries. Coverage is
never truncated to meet a time budget. Two ten-shard trials took 6m20s and 6m21s from workflow start to the final
gate. A twelve-shard hosted-seeded run took exactly five minutes. Sixteen shards
are the next measured candidate, retaining half on each architecture. Timeouts include setup margin and never truncate coverage.

## Per-test cost limits

`tools/ci/gate_budget.json` sets a 20-second default. Its bootstrap slow list
uses hosted canary timings, not the PrismaBuild-worker inventory. Runs
41240788649, 41242040262, 41242876374, 41243455746, 41244414908, 41245236880, 41245684824 and 41246084806 measured
13 tests above 20 seconds. Every exception is `ceil(2 * largest_hosted_time)`,
ranging from 44 to 116 seconds. The policy records the maximum, run ID,
artifact name and verified download digest for each entry. These measurements
include an earlier 49.002-second duplication audit and 37.809-second history
audit, larger than the latest 21.952 and 34.428-second observations.

Issue #2448, under #2328, owns reducing these costs and shrinking the slow list.
The per-file scheduling table still uses the PB inventory's summed medians;
it estimates shard balance, rather than setting per-test cost limits.

The gate runner checks xunit1 JUnit times, including setup and teardown, after
pytest completes. It prints every overrun with its precise node ID and limit.
The policy may only shrink versus `plan.base_sha`, the event's
`pull_request.base.sha`: neither the default nor an existing exception may
increase, and no new exception may be added. Removing an exception is allowed.
A target commit predating the policy permits bootstrap; an unavailable target
object fails with no verdict. Missing or malformed timing evidence also yields
no verdict, unless pytest already reported a test failure, which stays final.
Budget failures are final exit-1 verdicts even if pytest itself exited zero.
The 300-second test timeout remains a hang bound, separate from cost limits.
Budget enforcement runs only in this CI runner.

## Verdicts and retries

Exit 0 means pass, 1 means a test or cost-policy failure, and 75 means no verdict. Logs distinguish
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

Both `test_record_compatibility.py` and
`test_current_receipt_compat_composition.py` were tested on hosted x64 and arm64
with bubblewrap installed. Both fail namespace entry with `setting up uid map:
Permission denied`, before executing or attesting the retained consumer. They
now belong to the separate `namespace_compatibility` lane, owner #2447. The
PrismaBuild refusal reported by the selection agent is different, exit 78
because bubblewrap cannot mount /proc. Neither refusal is classified as flaky.
The three flaky bundle-cache cases under #2435 exhaust their file's collection,
so the quarantine names that complete file.

Hosted privilege checks are measured, not inferred. Canary 41245684824 changed
`kernel.apparmor_restrict_unprivileged_userns` from 1 to 0 on both architectures;
bubblewrap then entered its namespace but its deliberately hidden `/opt` tree
also hid setup-python. Canary 41246084806 used a byte-preserving runtime copy
under `/usr/local`; the next refusal named the shared libpython loader path.
The hosted setup registers that copied library directory with the loader and
relocates its installation metadata, including DESTSHARED, to the actual copied
standard-library directory. Canary 41246932853 passed all eight selected-runtime
controls on both architectures with the root-owned copy, restoring the two
previously deferred customization controls. Its namespace run then exposed the
remaining extension-module metadata path into hidden `/opt`.
These are disposable hosted-VM settings, with no fleet service changes.

The same canaries measured the complete 227-case root population with real
passwordless sudo. Earlier x64 runs passed 226 cases and ARM passed 224; run
41246932853 passed 224 on both. Both require the selected
`proof_container_runtime.run` wrapper for the installed-reader credential-drop
case, and ARM also failed two real-census assertions. Sudo itself works. The
authoritative root lane remains intact under its pinned proof-runtime wrapper
through PrismaBuild, owned by #1831, rather than pretending the bare-host run
provided that runtime attestation. Root privileges alone are not the remaining
requirement, and these root cases are not classified as flaky by this gate.

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
