# Run the backlog census

Use `fetch_open_issues.py` to save read-only GitHub evidence. Use `census.py` to
produce the census and the Phase 1 dry-run list from that snapshot. Neither tool
changes issues. The collector requires an authorized `gh` installation. The
analysis and tests require Python 3.10 or later and the standard library.

## Collect and report

Choose a new evidence directory for each collection. Keep evidence outside the
source checkout so later results do not change a fleet action's source identity.
PRI-2 publishes prismabuild only. Repeat `--repo` to select other assigned
repositories, or omit it to collect all three.

```bash
python3 tools/backlog_census/fetch_open_issues.py \
  --repo RobTand/prismabuild --since 2026-10-18T00:00:00Z --output /path/to/evidence/snapshot.json
python3 tools/backlog_census/census.py \
  --snapshot /path/to/evidence/snapshot.json \
  --since 2026-10-18T00:00:00Z --until 2026-10-25T00:00:00Z \
  --output /path/to/evidence/initial-report
```

The collector saves paginated raw replies beside the snapshot in `snapshot.raw/`.
It fetches all open issues, issues updated since the window start, referenced
parents, and dependencies. Missing access, API errors, and incomplete pagination
stop collection. It records that a multi-request collection is not atomic.
External repositories outside the three assigned repositories remain explicit
relationship gaps. Unavailable dependency reads leave blocker coverage incomplete
and add a limitation while collection continues.

The reporter writes `census.md`, `census.json`, `phase1-write-list.md`,
`phase1-write-list.json`, and a copy of `snapshot.json`. Rerun it offline against
the same inputs to regenerate these files.

## Cross-check candidates

Run the recall helper on the coordinator, where the existing Hindsight tunnel
and token file are available. This reads the fixed `omp-history` bank.

```bash
python3 tools/backlog_census/hindsight.py \
  --census /path/to/evidence/initial-report/census.json \
  --snapshot /path/to/evidence/snapshot.json \
  --output /path/to/evidence/hindsight
```

The helper saves one recall per candidate or structural write row. It resumes
saved inputs without repeating recalls and refuses changed inputs or receipts.
A hit is a lead. Open its cited source before relying on it. Record source
URLs, the finding, and any invalidation receipt in that subject's `index.json`
entry. Set `sources_reviewed` to `true` only after this review. A miss does not
prove the claim false. Invalidate a specific memory only when source evidence
contradicts its fact, using the authorized Hindsight PATCH operation. Never
delete memories or invalidate an old dated count because the backlog changed.

Regenerate the final report with the saved cross-checks:

```bash
python3 tools/backlog_census/census.py \
  --snapshot /path/to/evidence/snapshot.json \
  --hindsight /path/to/evidence/hindsight/index.json \
  --output /path/to/evidence/reviewed-report
```

Review every candidate against GitHub sources. A duplicate cluster means title
similarity, not an exact duplicate. The tool never closes on similarity. A fix
row requires an explicit PR closing reference, its merge commit on the saved
default branch, and matching default-head evidence. Review reopenings and the
issue's current scope before proposing closure. Unavailable native blocker requests are recorded as limitations; collection
continues with `blockers_complete: false`. Unknown blockers, empty blocker
sets, unsupported declarations, and incomplete comments prevent label removal.
All Phase 1 rows require the separate Chief of Staff audit before any write.

## Read the rollups and flow

An exact priority label wins over the leading title prefix. Only P0, P1, and P2+
are in scope. Native parents win over text. Text parents must begin a line with
`Part of`, `Sub-issue of`, or `Parent:`, with an optional list marker. Follow known closed
intermediate ancestors to the topmost open ancestor. Ambiguous links, cycles,
conflicts, and missing ancestors appear in diagnostics. Cycles use a stable open
representative. Each ticket appears once. A work unit takes its highest member
priority and belongs to its root repository.

Flow uses `(since, until]`. Ticket event counts include reopenings, using the
issue's priority at the endpoint. These counts are not historical priority
reconstruction. Missing timelines make the event net unknown. For exact changes
in open ticket and work-unit membership, pass `--before` with the earlier
snapshot. Set `--since` and `--until` to the two snapshots' `collected_at` values.
The report distinguishes entered and departed membership from issue events.
Parent or priority changes can alter membership without an opening or closure.
Without both endpoints, the report explicitly leaves unit flow unknown.

## Compare the planning snapshot

```bash
python3 tools/backlog_census/census.py \
  --planning-raw /path/to/planning/raw \
  --planning-time 2026-10-25T00:00:00Z \
  --baseline-report /path/to/planning/census.json \
  --output /path/to/evidence/planning-comparison
```

The original collector did not record its collection time. `--planning-time`
supplies provenance for that legacy input; it does not establish a live census
or a precise historical endpoint. Closed native parent states survive import,
but their ancestry remains unknown. The old native-parent records omitted the
repository, so import assumes the issue's repository. See [the baseline
reconciliation](BASELINE.md) for the reproduced counts and each root change.

## Verify through PrismaBuild

Submit the complete stdlib suite as one small execution unit. It uses one CPU,
one GiB aggregate memory, zero GPUs, and one native thread per library. The
portable command requires only the worker's Python standard library.

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --cwd /path/to/task-worktree --anywhere --priority -10 \
  --cpus 1 --demand mem_gb=1,gpu=0 \
  --env OMP_NUM_THREADS=1 --env MKL_NUM_THREADS=1 \
  --env OPENBLAS_NUM_THREADS=1 --env PYTHONDONTWRITEBYTECODE=1 \
  --timeout-s 180 --wait-s 600 -- \
  /usr/bin/python3 -m unittest discover -s tools/backlog_census -v
```

Inspect the terminal record, immutable attempt logs, cleanup, and content-verified
CAS receipt. Tests exercise the real CLI plus graph, scope, flow, conservative
write rules, collector pagination, default-branch ancestry, and recall recovery.
Offline fixtures do not qualify the live GitHub collector or complete the
candidate source audit.

## New tickets

Use the shared matcher and creation gate in [INTAKE.md](INTAKE.md). It owns native
child creation, priority evidence checks, and the repo-local to-tickets integration.
