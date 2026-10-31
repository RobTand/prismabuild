# Nightly full CPU suite

GitHub Actions runs the full CPU suite nightly at 03:17 UTC and on manual
`workflow_dispatch`. CI uses GitHub-hosted runners exclusively. Mergify remains
the merge queue. This PR stacks on #2433 and reuses its population declarations,
duration partitioner and `gate_run.py`; it does not change the gate's files.

The CPU population includes quarantined tests and the two-CPU population. Only the root and GPU populations declared by
`tools/verify.py` and its proof profiles are excluded. The initial matrix has
16 duration-balanced shards assigned round-robin across `ubuntu-latest` and
`ubuntu-24.04-arm`, with four xdist workers per job and bounded native threads.
Sixteen jobs leave capacity within the public repository's approximate
20-job allowance. The inventory's roughly 4,000 CPU-seconds
suggests several minutes of test execution plus setup, targeting ten minutes.
Hosted measurements, rather than that estimate, determine later adjustments.

One concurrency group permits one active run and one pending run, with
`cancel-in-progress: false`. A pytest test failure is never retried. The runner
retries a shard once only when it ends without a complete test verdict, using
the same exit-code contract as the gate. Dependency installation also gets one
infrastructure retry. Missing runner or setup output leaves reporting incomplete.

## Artifacts and reporting

The plan artifact records the exact tested SHA and every expected runner,
shard and file. Each shard recomputes its assignment through the gate partition
CLI from its own checkout; no file list passes between jobs. Each shard publishes its runner verdict and legacy JUnit XML,
including after failure. The final report job runs with `if: always()` and the
workflow's own token with `issues: write` and `actions: read`.

Before reporting, it requires every expected shard's matching SHA, assignment,
terminal test verdict and JUnit file. Missing or invalid input writes an
incomplete snapshot and files nothing. Such snapshots cannot become history.
Duplicate tests within an architecture refuse reporting.

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
