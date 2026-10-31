# Full-suite failure reporting

CircleCI owns PB's CI. The full suite will run after pushes to `main` and on a
schedule, using CircleCI's timing-based test splitting and `store_test_results`.
Mergify remains the merge queue. The CircleCI resource classes and config are
pending the platform agent's example in #2428. This change installs no runners
and contains no test-execution workflow.

## Reporter input and decisions

`tools/ci/full_suite_report.py` accepts a directory of JUnit XML files and an
explicit previous `results.json`. Its parser, classification, owner lookup,
issue rendering, and recovery planning are pure functions. Filesystem and REST
calls are adapters around those decisions.

JUnit must preserve repository-relative Python file paths. For pytest, use
`-o junit_family=legacy --junitxml=<file>` so each testcase has a `file`
attribute. An explicit `nodeid` testcase property also works and takes
precedence. Class names and parameter IDs remain part of the test identity.
Ambiguous XML, missing outcomes, and duplicate tests across files refuse
reporting. Setup and teardown failures for one test remain one failed outcome.

A failure is new if it did not fail in the previous results. Continued failures
file nothing. `tools/ci/gate_quarantine.json`, when present, suppresses new
filing for quarantined tests, while keeping their outcomes in the results.
The full-suite selection must retain those tests. Only the `root` and `gpu`
populations in `tools/verify.py` are excluded by the eventual CircleCI config.

A new failure creates a `[P2] Full suite: … fails on main` issue with the
`full-suite` label and the existing `P2` priority label. A hidden exact test
marker identifies its open owner. Recurrences comment on that owner instead.
A test that passes again comments `passed at SHA` without closing the issue.
Skipped and absent tests are not recoveries. PRs cannot own test failures.
More than 20 new failures produce one summary issue.

Issue text includes the commit range, merged PRs when available, the first
failure lines, the run URL, and a PrismaBuild reproducer. Mention characters
are replaced. REST request bodies and saved results redact the runtime GitHub
token. Repeated writes reuse owners and avoid identical comments.

The previous snapshot must be complete and have performed reporting. A dry run
cannot become the next comparison baseline, because its failures were never
filed. `--incomplete` records an incomplete result without filing or claiming
recoveries. The CI adapter must determine completion from all expected reports,
not from the test command's exit code alone.

## CircleCI public API evidence and hosting choice

Checked on 2026-10-31 without reading credentials or sending authorization:

- `GET https://circleci.com/api/v2/project/gh/numpy/numpy/60837/artifacts`
  returned HTTP 200 with 5,538 artifact records. Following the public
  `.buildinfo` artifact URL returned 231 bytes after a 302 redirect and HTTP 200.
- The corresponding `/60837/tests` endpoint returned HTTP 200 with an empty
  `items` array. PB's `/1/artifacts` and `/1/tests` also returned HTTP 200 with
  empty arrays. These empty responses establish anonymous endpoint access;
  they do not prove access to a nonempty PB test-results population.
- Public pipeline metadata for NumPy pipeline
  `478b0bc5-0566-4b54-b134-f533c93db138` returned its project, `main` branch,
  and exact Git revision without a token.

CircleCI's [API developer guide](https://circleci.com/docs/guides/toolkit/api-developers-guide/)
documents token authentication and uses a personal token in its artifact
examples. Its [v1 F/OSS documentation](https://circleci.com/docs/api/v1/#foss)
explicitly describes anonymous reads for some public-project endpoints, subject
to the project's F/OSS setting. The successful v2 reads above are observed
behavior, not a blanket promise of anonymous access to every v2 endpoint.

Recommend a hosted GitHub Actions reporting job with its own `GITHUB_TOKEN`.
It downloads public CircleCI JUnit artifacts as data and uses GitHub REST to
file issues. This needs no new CircleCI or GitHub secret. Running the reporter
inside CircleCI would instead need a GitHub credential added to that project.
The bridge uses raw XML artifacts and does not depend on nonempty `/tests`
metadata access. Anonymous access must still be proven on PB's first real
full-suite build. API errors and missing artifacts fail without advancing history.

CircleCI's [test-splitting guide](https://circleci.com/docs/guides/optimize/use-the-circleci-cli-to-split-tests/)
documents `circleci tests split --split-by=timings` and the timing data supplied
by `store_test_results`. That covers the custom partitioner's responsibility,
so the partition tool was removed. The [test-results guide](https://circleci.com/docs/guides/test/collect-test-data/)
distinguishes `store_test_results` from `store_artifacts`; raw JUnit downloads
need the latter too. Suite pruning remains tracked in #2328.

## Bridge contract for the later CircleCI config

A final job named `full-suite-report-inputs` collects all CPU-suite JUnit files
under `full-suite/junit/`. It stores those files as artifacts and writes
`full-suite/run.json` as another artifact:

```json
{
  "schema": 1,
  "sha": "<full tested Git SHA>",
  "branch": "main",
  "complete": true,
  "junit": ["full-suite/junit/0.xml", "full-suite/junit/1.xml"]
}
```

`complete` means every expected shard delivered its final JUnit. A test failure
is still a verdict. Missing infrastructure verdicts must not set `complete`.
The final job must run after failures too and publish its terminal status.
It supplies the expected artifact set explicitly so an incomplete download
cannot masquerade as a full suite. No resource class is assumed here.

The hosted `full-suite-report.yml` bridge reacts to CircleCI status or check
completion. It checks the CircleCI project, job name, branch, and exact commit
before downloading only the declared XML files. It executes the trusted
reporter from `main`, never code from a CircleCI artifact or event SHA.
Its previous snapshot is the last complete, reported `full-suite-results`
artifact from this bridge. Older or repeated CircleCI pipelines cannot replace
a newer baseline. Other CircleCI jobs are ignored without polling.

## Run the reporter without a CI provider

An offline dry run needs no token and performs no network requests:

```bash
python3 -I tools/ci/full_suite_report.py \
  --input <junit-directory> --previous <previous-results.json> \
  --output <results.json> --sha <tested-sha> --run-url <ci-run-url> --dry-run
```

Omit `--previous` only when bootstrapping without history. Use `--issues` and
`--merged-prs` to supply optional JSON snapshots for offline rendering.
For real filing, omit `--dry-run`, supply `--repository RobTand/prismabuild`,
and provide `GITHUB_TOKEN` through the environment. No credential is accepted
on the command line. Exit codes are 0 for passing outcomes, 1 for test failures,
and 2 for incomplete or invalid input and reporting errors.

Development tests still use the published PrismaBuild client at priority -10
from a clean committed checkout. Check actual verdicts, terminal logs, and CAS
receipts. CircleCI jobs execute CI directly.
