# Land a pull request through the merge queue

Every change reaches `main` through Mergify's merge queue. CI runs as
GitHub Actions checks on GitHub-hosted runners, and Mergify gates on them.
The configuration is `.mergify.yml`; the CI jobs are in
`.github/workflows/gate.yml` and described in [the CI gate guide](ci_gate.md).

## Before you queue

1. Name the branch `prismabuild-<issue>` or `prismabuild-<issue>-<word>`, for
   example `prismabuild-123-tl3-2`.
2. Start the PR body with a `Change type:` line and reference the branch's
   issue, for example `Refs #123`. `Fixes #123` also closes the issue on merge.
3. Wait for two checks to pass:
   - `issue-link` checks the branch name and the issue reference;
   - `pr-gate` runs the tests selected from your changes.

The optional `regression-test` check also runs. Run your development tests
through PrismaBuild and cite the evidence in the PR body; `pr-gate` is a
cheap first check, not a substitute.

## Queue and dequeue

- Add the `queue` label. Mergify adds `queued` when the pull request enters
  the queue.
- Remove the `queue` label to take the pull request out of the queue.
- Add `release` as well to use the release queue, which is served first.
- The comment commands `@mergifyio queue` and `@mergifyio requeue` also work, for
  people with write permission and for TL3's GitHub App (`tl3-robtand[bot]`),
  which requeues its own pull requests after a dequeue.

## What happens in the queue

Mergify groups queued pull requests into batches. In the default queue a batch
holds one pull request when the queue is short and grows to seven when it
backs up; the release queue always tests one pull request at a time. For each
batch, Mergify opens a draft pull request on a `mergify/merge-queue/` branch
that contains `main`, the batches queued ahead, and this batch. Two batches
are tested at once.

On the draft, the `gate` check runs the full gate population on the
integrated SHA, split by recorded test durations across GitHub-hosted runners
of both architectures. `issue-link` also
runs on the draft. It accepts the draft only when Mergify opened it and every
pull request in the batch passes the ordinary rule.

When `gate` passes, Mergify merges the batch's pull requests into `main`.
The gate reruns a shard once when it ended without a test verdict, such as a
lost runner. When `gate` fails, Mergify splits the batch to find the pull
request that broke it, merges the others, and removes the
culprit from the queue with the `dequeued` label and a comment that links the
failing run.

## When the gate fails

A pull request that left the queue after a failed check stays out of it, even
with the `queue` label still on. Removing the label and adding it again does
not bring it back. Comment `@mergifyio queue` instead; Mergify then queues it
as soon as its `pr-gate` check passes. The older `@mergifyio requeue` still
works but is deprecated.

- **A test in this pull request failed.** Open the failing shard in the `gate`
  run, read the test, fix the pull request, push, then comment
  `@mergifyio queue`.
- **The failure is not this pull request's**, for example a flaky test in an
  unrelated file. Mergify's comment says whether the pull request was tested
  alone. Make sure the flaky test has an owning issue and a quarantine entry
  (see docs/ci_gate.md), then comment `@mergifyio queue`.
- **A job ended without a test verdict**, for example a lost runner or a
  failed setup step. Its log says so. After the runners recover, comment
  `@mergifyio queue`.
- **A test fails that your change can't affect.** Check the issues labelled
  `full-suite` for a known failure on `main`, and link it in your pull
  request.

Never merge outside the queue.
