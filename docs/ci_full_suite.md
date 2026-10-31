# Nightly full CPU suite

GitHub Actions runs the full CPU suite nightly at 03:17 UTC and on manual
`workflow_dispatch`. CI uses GitHub-hosted runners exclusively. Mergify remains
the merge queue. This PR stacks on #2433 and reuses its population declarations,
duration partitioner and `gate_run.py`; it does not change the gate's files.

The CPU population includes quarantined tests and the two-CPU population.
The root and GPU populations stay in their pinned lanes. Capability-only
controls recorded by the gate also stay out: fleet broker/runtime and archival
replay inputs are unavailable on hosted runners. Precise node exclusions retain
portable cases in mixed files. The plan records every such exclusion with its
owning issue. Ordinary quarantine never removes nightly coverage. The matrix has
16 duration-balanced shards assigned round-robin across `ubuntu-24.04` and
`ubuntu-24.04-arm`, with four xdist workers per job and bounded native threads.
Sixteen jobs leave capacity within the public repository's approximate
20-job allowance. The first full hosted trial finished in 300 seconds, below
the ten-minute target. Hosted measurements determine later adjustments.

One concurrency group permits one active run and one pending run, with
`cancel-in-progress: false`. A pytest test failure is never retried. The runner
retries a shard once in a fresh job only when it ends without a complete test
verdict, using the gate attempt planner and aggregator. This covers checkout,
setup and runner loss. The first definite test verdict is final. Missing output
after retry leaves reporting incomplete. The final job preserves the gate
aggregator's failure, including cost-policy failures, while still running the
reporter and uploading its artifact. `FULL_SUITE_SHARDS` is the single
workflow shard-count setting.

## Artifacts and reporting

The plan artifact records the exact tested SHA and every expected runner,
shard and file. Each shard recomputes its assignment through the gate partition
CLI from its own checkout; no file list passes between jobs. Each shard publishes its runner verdict and legacy JUnit XML,
including after failure. The final report job runs with `if: always()` and the
workflow's own token with `issues: write` and `actions: read`.

Before reporting, it requires every expected shard's matching SHA, assignment,
terminal test verdict and JUnit file. Missing or invalid input writes an
incomplete snapshot and files nothing. Such snapshots cannot become history.
Duplicate tests across shards refuse reporting.

Every CPU test file runs once across the mixed-architecture matrix. A test
recovers only when it passes; absent and skipped tests cannot establish recovery.
Runner identity, outcomes and durations remain in the `full-suite-results` artifact, retained for 90 days.

The reporter compares against the latest complete, non-dry-run results artifact
from this workflow on `main`. Dry runs never suppress later issue filing.
A new failure creates a `[P2] Full suite: … fails on main` issue with a
`full-suite` label. Exact hidden test markers identify existing open owners;
recurrences comment on them. Recovery comments say `passed at SHA` without
closing the owner. More than 20 new failures create one summary issue.
Quarantine and cost-exclusion entries suppress new filing, never execution.

Issue text includes the commit range, merged PRs, first failure lines, run URL
and a PrismaBuild reproducer. Mention characters are replaced. Runtime token
values are redacted from REST payloads and output artifacts. GitHub I/O uses
REST only; signed artifact downloads never receive the authorization header.

## Rerun

Run the `Full suite` workflow from GitHub's Actions page, or use REST:

```bash
gh api -X POST repos/RobTand/prismabuild/actions/workflows/full-suite.yml/dispatches \
  -f ref=main -F inputs[dry_run]=true
```

The `dry_run` input classifies without creating issues or comments. Branch runs
are always dry runs. Production execution is nightly, not on every main push.

The reporter also works independently of Actions:

```bash
python3 -I tools/ci/full_suite_report.py \
  --input <junit-directory> --previous <previous-results.json> \
  --output <results.json> --sha <tested-sha> --run-url <run-url> --dry-run
```

An offline reporter dry run needs no token. Optional `--issues` and
`--merged-prs` JSON snapshots support rendering with existing metadata. Real
filing takes `GITHUB_TOKEN` from the environment. JUnit needs a `nodeid`
property or pytest legacy `file` attributes; use `-o junit_family=legacy`.
Exit codes are 0 for passing tests, 1 for test failures, and 2 for incomplete or
invalid reporting input.

Development tests use the published PrismaBuild client at priority -10 from
clean committed worktrees. Their verdicts, terminal logs and CAS receipts are
verified separately from hosted CI evidence. Suite pruning is tracked in #2328.

Hosted setup matches the gate, including bubblewrap, the Ubuntu user-namespace
setting, a root-owned copy of the selected interpreter under `/usr/local`, and
short private test scratch outside the checkout. No fleet service is installed.

## Hosted proof

The temporary PR trigger was limited to this branch and repository. It is
removed before review, together with the deliberate failure and proof-only
population settings.

The first complete dry run, [41250497075](https://github.com/RobTand/prismabuild/actions/runs/41250497075),
used 16 shards and finished in 300 seconds from workflow creation to completion.
It covered 1,261 files and reported 19,803 passed, 104 skipped and 13 failed.
Classification was 2 new, 11 quarantined, 0 known and 0 recovered. It filed
nothing and needed no infrastructure retry. The two new failures were the
repository duplication checks: an unnecessary sorted-JSON serialization and
a classifier name collision introduced here. Both were repaired without
changing the duplication baseline.

| Shard | Architecture | Job seconds | Test-step seconds |
| --- | --- | ---: | ---: |
| 0 | x64 | 241 | 161 |
| 1 | ARM | 187 | 116 |
| 2 | x64 | 233 | 162 |
| 3 | ARM | 121 | 64 |
| 4 | x64 | 197 | 92 |
| 5 | ARM | 180 | 81 |
| 6 | x64 | 146 | 91 |
| 7 | ARM | 133 | 73 |
| 8 | x64 | 198 | 125 |
| 9 | ARM | 160 | 94 |
| 10 | x64 | 236 | 148 |
| 11 | ARM | 119 | 61 |
| 12 | x64 | 204 | 140 |
| 13 | ARM | 138 | 80 |
| 14 | x64 | 211 | 117 |
| 15 | ARM | 134 | 72 |

After those repairs, [41251120643](https://github.com/RobTand/prismabuild/actions/runs/41251120643)
completed the same 1,261-file, 16-shard population in 311 seconds. It reported
19,805 passed, 104 skipped and 11 failed, all quarantined. Classification was
0 new, 11 quarantined, 0 known and 0 recovered. It filed nothing and needed no
infrastructure retry. The shard timings were:

| Shard | Architecture | Job seconds | Test-step seconds |
| --- | --- | ---: | ---: |
| 0 | x64 | 215 | 153 |
| 1 | ARM | 170 | 118 |
| 2 | x64 | 198 | 140 |
| 3 | ARM | 128 | 66 |
| 4 | x64 | 224 | 136 |
| 5 | ARM | 128 | 78 |
| 6 | x64 | 214 | 141 |
| 7 | ARM | 134 | 75 |
| 8 | x64 | 157 | 84 |
| 9 | ARM | 144 | 91 |
| 10 | x64 | 190 | 137 |
| 11 | ARM | 126 | 63 |
| 12 | x64 | 232 | 141 |
| 13 | ARM | 155 | 73 |
| 14 | x64 | 179 | 113 |
| 15 | ARM | 129 | 67 |

The real filing proof, [41250910808](https://github.com/RobTand/prismabuild/actions/runs/41250910808),
ran one isolated deliberate failure through the real gate runner and reporter.
It finished in 129 seconds, with a 72-second shard job and a 2-second test step.
Classification was 1 new, 0 quarantined, 0 known and 0 recovered. The workflow's
own token created [issue #2452](https://github.com/RobTand/prismabuild/issues/2452)
with the exact marker, failure excerpt, SHA and PrismaBuild reproducer. The
issue was closed after verification with a one-line explanation. The failed
test was not retried.

The 104 skips and capability exclusions remain visible in the artifacts.
Missing `py-spy` is among the quarantined failures; the workflow does not claim
those tests passed. Production baseline retrieval and recovery comments are
covered by deterministic tests; the proof did not seed a results artifact on
`main` or exercise a real recovery comment.
