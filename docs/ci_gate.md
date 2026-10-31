# PB merge gate

The `gate` check tests the integrated head of a Mergify batch draft. Ordinary
PR pushes create no gate job on a runner. Development tests still run through
PrismaBuild at priority -10. GitHub Actions CI runs directly on the fleet's
separate `pb-gate` runner slots.

This implementation is pending qualification under #2428. The three-repeat
inventory at `22e9d75298608078c91d331a646f6446352577f6` must supply measured
durations and current failures. The lead must also authorize a run on live
`pb-gate` slots before enabling this check as a queue condition. An empty
duration table means unknown cost. It does not establish the ten-minute target
or a clean population.

## Selection

`tools/ci/gate_select.py` reads the PR event's base and head object IDs.
It compares those two trees, so the batch's queued-ahead changes are included.
It never checks GitHub's synthetic merge ref. Only the two commit objects are
needed, which keeps checkout shallow. Renames retain both old and new paths.

The selector starts with `FAST` from `tools/verify.py`. It also selects changed
or added test files, direct importers of changed Python modules in
`src/prismabuild/` and `tools/`, and tests with literal references to changed
paths. Imports include aliases, dynamic module-name strings, Python snippets,
and literal `Path` joins. The scanner reads ASTs and never imports tests.
It does not follow transitive imports. Dynamic paths assembled from variables
can escape the scan. The full suite remains responsible for coverage outside
this evidence.

Changes to conftests, `pyproject.toml`, `tools/verify.py`, root population
manifests, test helper modules, `tools/ci/gate_*`, or the gate workflow select
the entire eligible population. This includes the gate tests and this document.
Test helper modules are Python files under `tests/` whose names do not start
with `test_`, plus test modules imported by other tests.

The root and GPU populations stay in their existing lanes. The gate applies
file and node-ID exclusions from `tools/ci/gate_quarantine.json`. Every entry
has a reason and an open owning issue. Historical clusters whose repair issues
are closed name #2428 as their temporary requalification owner and retain the
original issue in `source_issues`. They are not labelled as newly observed
flakes. Archival replay files require inputs absent from the image and name
#1997. Root profile renewal is tracked by #1831. Quarantine is containment,
not a fix, and the full suite must keep these tests.

The JSON plan records selection reasons, deferred tests and owners, node-ID
deselections, a population version, and estimates. Missing durations propagate
as unknown. Known costs use longest-first file packing over five workers,
matching `--dist=loadfile`. A partially quarantined file keeps its full measured
cost, which is conservative. Estimates exclude checkout, collection, xdist
startup, artifact upload, and runner pickup. The selector never cuts relevant
tests to make an estimate fit the budget.

## Verdicts

Both attempts run on `[self-hosted, pb-fleet, pb-gate]`. The same-repository
check applies before any self-hosted job. A batch must also have a
`mergify/merge-queue/` head ref, be a draft, and have the exact Mergify bot login,
numeric ID 37929162, and `Bot` account type. A same-repository PR with
`gate-canary` can exercise the workflow before the queue uses it. Adding that
label alone does not trigger a run. Push a commit or reopen the PR afterward.
Never use a fork PR for a canary.

The runner image provides Python and pytest. Each attempt runs at most five
xdist workers, bounded by its inherited CPU affinity, with one native thread
per worker and `PRISMABUILD_TEST_TIMEOUT_S=60`. It preserves the slot's CPU mask.
The summary explains the selection. The artifact contains the JSON plan and
JUnit XML, with skips reported by pytest.

A completed pytest exit writes `pass` for exit 0 or `fail` for any other
nonnegative exit code. Collection, usage, internal, and no-tests errors fail
the gate without retry. A launch error, lost runner, signal termination, or
checkout failure writes no verdict. Only an absent first verdict starts the
second attempt. There is no third attempt. The final hosted job named `gate`
uses the first available verdict and fails if both attempts produced none.
A test failure is never retried. Artifact failure after a test verdict does
not cause tests to run again.

Each attempt has a ten-minute execution limit. That limit is containment,
not evidence that draft-to-verdict p90 is under ten minutes. Runner pickup and
an infrastructure retry add time. Queue enablement needs observed canary and
queue timings, not the configured timeout alone.

## Maintain exclusions and costs

To add an exclusion, record the exact file or pytest node ID in
`tools/ci/gate_quarantine.json`, with `reason`, a positive integer `issue`, and
its evidence. Verify that the issue is open through GitHub REST. If no open
issue owns the failure, create one with the goal and acceptance criteria.
A file entry removes that file. A node-ID entry becomes a pytest `--deselect`
prefix, so an unparameterized test ID excludes all of its parameters.

Fix the cause with a deterministic regression test. To remove an entry, attach
the required repeated PrismaBuild passes and a CI-container qualification to
its owner, then delete the entry in a reviewed PR. Closing an old issue alone
does not requalify a test for this image. Keep full-suite coverage throughout.

Put measured per-file seconds in `tools/ci/gate_durations.json`, with the source
commit, repeat evidence, architecture, and aggregation method. Use the completed
three-repeat inventory, not a single run's outcomes. Put any cost exclusion in
`cost_exclusions` with its measured reason and an open owner. No cost exclusion
is inferred from absent duration data.

Capture replay inputs through read-only REST from a fresh destination:

```bash
python -I tools/ci/gate_replay.py --fetch-history /path/to/fresh/history
```

Run the replay through published PrismaBuild from a clean committed checkout.
Its offline `--history /path/to/history.json --output /path/to/replay.json`
mode scans the current population once, then replays the latest 150 merged PRs.
The capture paginates closed PRs until it proves the merge-time boundary, and
paginates each file list. It refuses the REST 3,000-file ceiling. Percentiles
use nearest rank. This estimates current policy on historical changes, not
historical runtime or regression-detection accuracy. Missing costs make the
wall-time percentiles unknown. Replay evidence must precede a budget policy
change. Dense direct imports of `pool` or `core` may require finer evidence,
such as nightly `sys.monitoring` function maps. Do not trim those importers
without a measured and reviewed replacement.
