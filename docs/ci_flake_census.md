# Flake census

The manual Flake census workflow freezes main's SHA once and collects five
independent samples per test on each architecture by default. `repeats` accepts
2 through 10. Every file runs on both `ubuntu-24.04` and `ubuntu-24.04-arm`
every repeat. Four duration-balanced shards per architecture use the gate's
partitioner, hosted setup and runner with four xdist workers each.

The matrix permits at most four simultaneous test jobs, leaving sixteen of
the approximate twenty hosted slots for a queue gate. It shares the nightly's
`full-suite-main` concurrency group, so a census and nightly cannot consume
eight slots together. Runs are never cancelled to make room for a newer run.
Each repeat uses fresh jobs. A failing test is a sample, never an infrastructure
retry. Missing setup, runner or artifact evidence makes the census incomplete
and prevents issue filing.

The observed population is the gate's eligible CPU files plus its quarantined
and cost-excluded tests. They must execute for quarantine retirement evidence.
The gate's special lanes and unavailable hosted capabilities remain excluded,
including archival replay. The artifact preserves those exclusions and the
quarantine snapshot from the tested main SHA. The census never changes policy.

The classifier counts passed, failed, skipped and missing observations per
architecture. A flake requires both pass and fail in repeats on the same
architecture. Stable failures and architecture differences are separate groups.
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
failure excerpts are indented; no user mentions are emitted.

```bash
gh api -X POST repos/RobTand/prismabuild/actions/workflows/flake-census.yml/dispatches \
  -f ref=main -f 'inputs[repeats]=3' -f 'inputs[dry_run]=true'
```

Branch proof runs use branch tooling but still test the frozen main checkout.
They always classify without filing. Test failures do not make a complete
census fail: detecting those failures is its purpose. Infrastructure gaps do.
