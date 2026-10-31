# Full suite after merges and nightly

`full-suite.yml` runs after pushes to `main`, nightly at 02:23 UTC, and through
`workflow_dispatch`. One concurrency group keeps one active run and the newest
pending run. This workflow runs off the Mergify merge path.

Four shards run on `[self-hosted, pb-fleet, pb-suite]`. Each slot has four CPUs
and 6 GiB of memory. Each shard runs four xdist workers with one native thread
each and a 300-second failure bound per test. The runner image provides Python
3.14, pytest 9.1.1, pytest-xdist 3.8.0, PyYAML, and Git. No shared fleet mount is
required or used by the workflow.

The CPU population comes from `tools/verify.py`. Only its `root` and `gpu`
lanes are excluded. Quarantined tests remain selected. Files are sorted by
previous measured duration and assigned to the lightest shard, with stable
lexical and shard-index tie breaks. Unknown files cost one second initially.
All shards use the same history artifact so they cover each selected file once.
The results artifact retains outcomes and durations per exact pytest node ID.

## Verdicts and retries

The second attempt skips any shard that already has a test verdict. A test
failure, including a collection error, is a verdict and is never retried.
Lost runners, failed checkouts, malformed JUnit, and crashes without a test
failure get one retry. The final report always runs. It fails when tests fail
or when a shard still lacks a verdict. The shard step itself returns success
when it delivers a verdict, including a failing verdict, so the report owns the
workflow's test status.

An incomplete run files or comments on one infrastructure issue. It cannot
replace a complete baseline or report recoveries. If history preparation fails,
the first attempt does not run. Retry shards use the same one-second defaults
when no history artifact exists.

## Automatic issues

The report uses GitHub REST with the workflow's `GITHUB_TOKEN`. It reads the
previous completed `main` run's `full-suite-results` artifact. If that run is
incomplete, was a dry run, or its artifact expired, it looks back for a complete
baseline that performed reporting.
Without a retained baseline, observed failures count as new.

A newly failing test gets one `[P2] Full suite: … fails on main` issue, labelled
`full-suite`. A hidden test marker identifies its open owner. Recurrences add
a comment to that issue. Continued failures file nothing. Tests listed in
`tools/ci/gate_quarantine.json`, when present, file nothing new. A passing test
comments `passed at SHA` on its owner without closing it. Skipped or missing
tests do not count as recoveries.

Each report includes the last passing commit or previous baseline, the commit
range and merged PRs, a failure excerpt, the run URL, and a PrismaBuild
reproducer. More than 20 new failures produce one summary issue with all test
markers. Issue text replaces mention characters. The report never logs the
workflow token or an artifact's signed download URL. Repeated report attempts
reuse open owners and avoid identical comments.

The `full-suite-results` artifact is retained for 90 days. It contains the
classification, shard timings, test outcomes, durations, and last passing SHAs.
Shard JUnit artifacts are retained for 14 days. The GitHub job summary shows
counts and shard timings. The Actions run view supplies total workflow wall
time. Duration history is available for later refreshes of the gate's table;
this workflow does not edit that table. Suite pruning is tracked in #2328.

## Rerun and inspect

Dispatch with REST, naming `main` or a branch containing the workflow:

```bash
gh api -X POST repos/RobTand/prismabuild/actions/workflows/full-suite.yml/dispatches \
  -f ref=main -F 'inputs[dry_run]=true'
```

`dry_run` defaults to true for manual dispatches. It reads owners and history,
classifies outcomes, and uploads results without writing issues or comments.
Set `inputs[dry_run]=false` for real filing. Push and schedule events on `main`
file automatically. Branch trial artifacts never become a `main` baseline.
GitHub enables workflow dispatch only after the workflow exists on its default
branch. Before then, an authorized branch-limited temporary trigger is needed
for live proof and must be removed before review.

Inspect `report/results.json`, each shard's `junit.xml`, the report job summary,
and the Actions run conclusion. A queued run or artifact upload alone proves
no test result.

To reproduce a reported test during development, commit first and use a clean
detached checkout. Submit through the published PrismaBuild client:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py \
  --checkout <clean-checkout> --python /home/rob/venvs/pb-cpu/bin/python \
  --workers-per-shard 1 --threads-per-shard 1 --mem-gb 4 --priority -10 \
  --timeout-s 1800 --test-timeout-s 300 --json <report-outside-checkout> \
  'tests/example.py::test_name'
```

Check the verdict, terminal record, logs, and CAS receipt. CI on the suite
runners runs directly and does not submit recursively through PrismaBuild.
