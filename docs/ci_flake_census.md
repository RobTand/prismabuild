# Flake census

The manual Flake census workflow freezes main's SHA once and collects five
independent samples per test on each architecture by default. `repeats` accepts
2 through 10. Every file runs on both `ubuntu-24.04` and `ubuntu-24.04-arm`
every repeat. Set the optional `quarantine_issue` input to a quarantine
owner number from `tools/ci/gate_quarantine.json` to repeat only that owner's
target files. It does not check that the issue is still open. Node-level entries select
their whole file, so sibling nodes also run. The plan rejects unknown owners or
unavailable target files, freezes the filter in its manifest, and rechecks it in
every assignment. Classification retains the complete quarantine snapshot.
For example, `quarantine_issue: '2434'` with `repeats: 5` supplies fresh
requalification evidence without rerunning unrelated files.

Four duration-balanced shards per architecture use the gate's
partitioner, hosted setup and runner with four xdist workers each.

The matrix permits at most four simultaneous test jobs. That leaves roughly
sixteen of the account's approximate twenty hosted slots available, without
reserving them for the merge queue. Other public CI, including TL3, may share
that account pool. Start a manual census only when the merge queue is quiet.
There is no weekly schedule. It shares the nightly's
`full-suite-main` concurrency group, so a census and nightly cannot consume
eight slots together. Runs are never cancelled to make room for a newer run.
Each repeat uses fresh jobs. A failing test is a sample, never an infrastructure
retry. Missing setup, runner or artifact evidence makes the census incomplete
and prevents issue filing. Completed shards retain their counts, excerpts
and disk measurements. Missing or interrupted shards contribute missing
observations to IDs seen in other samples; unavailable file assignments remain
listed in the artifact when no sample discovered their node IDs. A missing disk
record marks the report incomplete but preserves valid JUnit observations.

Hosted requirements include pinned `py-spy` for real sampling tests. The census
installs the control revision's requirements and logs the profiler version while
the test checkout stays at the frozen main SHA. Branch qualification can therefore
prove a proposed hosted dependency change against unchanged main tests.

The observed population is the gate's eligible CPU files plus its quarantined
and cost-excluded tests. They must execute for quarantine retirement evidence.
The gate's special lanes and unavailable hosted capabilities remain excluded,
including archival replay. The artifact preserves those exclusions and the
quarantine snapshot from the tested main SHA. The census never changes policy.

The classifier counts passed, failed, skipped and missing observations per
architecture. A flake requires both pass and fail in repeats on the same
architecture. Stable failures and architecture differences are separate groups.
`unresolved` means that the available samples do not establish any of those
patterns across both architectures. For example, a node that fails every repeat
on x64 but is absent on arm64 remains unresolved: inspect its x64 failures and
arm64 missing counts rather than interpreting the label as a pass. Only nodes
that both pass and fail on the same architecture qualify for new flake issues.
Skipped or absent tests never count as passes. Quarantined nodes retain all
counts but do not file issues; a node passing every sample on both architectures
is a retirement candidate for its named owner, not automatically unquarantined.

Before tests, each shard logs free bytes and inodes under its actual `TMPDIR`
using `df -B1`. The artifact and summary keep the measurement with architecture,
repeat and shard, so disk-dependent failures can be compared across runners.
Failures containing the queue-space-warning prefix are grouped as the known
environmental cause tracked by #2508 through #2514. The classifier scans the
whole failure text, including lines beyond its bounded excerpt, and suppresses
individual filings for that cause. Existing open owners also suppress filings.

The job summary lists findings and counts. The `flake-census-results` artifact
contains every observed node, counts, failure excerpts, sample URLs, population
SHA, runner evidence and filing decisions. Raw JUnit and runner records are
retained for 30 days; the classification artifact is retained for 90 days.

Dry runs are the default. Real filing is allowed only on a manual run of the
workflow on main with `dry_run: false`. The final report job alone receives
`issues: write`; the workflow defaults to `contents: read`. It uses GitHub REST
with the workflow token, without a new secret. Each new unquarantined
intermittent node gets one issue with evidence and a proposed
`tools/ci/gate_quarantine.json` entry. The proposal requires the newly created
issue number before it is valid policy. Existing open marker owners suppress
creation, including issues filed by the nightly. The census never comments on
an existing owner or closes it. IDs and titles are literal escaped text and
failure excerpts are indented; no user mentions are emitted. Quarantine JSON
and the Bash reproducer preserve exact IDs by escaping mention characters.

```bash
gh api -X POST repos/RobTand/prismabuild/actions/workflows/flake-census.yml/dispatches \
  -f ref=main -f 'inputs[repeats]=3' -f 'inputs[dry_run]=true'
```

Branch proof runs use branch tooling but still test the frozen main checkout.
They always classify without filing. Test failures do not make a complete
census fail: detecting those failures is its purpose. Infrastructure gaps do.
