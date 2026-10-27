# Batching merge queue (`pbmergeq.py`)

`tools/fleet/pbmergeq.py` tests a batch of pull requests with one full-suite
`pbtest.py` run and posts a commit status on each tested head (#1417). It
submits work only through the published `pbtest.py`; PrismaBuild keeps
sharding and placement.

## How a batch runs

1. Take up to `batch_cap` queued pull requests. Each must pass the
   eligibility filter: open, not a draft, a head branch in the repository
   itself (never a fork), opened by `author`, based on `base_branch`. Every enqueue requires the full lowercase SHA the reviewer approved. A moved
   head is refused with a comment requesting re-review and a new enqueue.
   Legacy unpinned entries are refused before testing.
2. Fetch the base branch and each full immutable commit selected from GitHub
   into a bare mirror the queue owns. Require exact destination-ref equality
   and a commit object; refuse failed fetches explicitly. Synthetic
   `refs/pull/N/head` may lag the real branch and are not the selected source.
   Preserve the independent enqueued-pin and subsequent GitHub head checks, and
   merge them in queue order onto a fresh worktree of the base. A pull request
   that conflicts with the base gets a comment and drops out; one that only
   conflicts with an earlier batch member is re-queued.
3. Run the suite once on the candidate with `pbtest.py --json`.
4. Judge by failure set. The verdict is the set difference of failing node
   IDs, candidate minus base. The base is run only on the files the candidate
   failed in, which is all the difference needs, and results are cached per
   source tree, selected interpreter/full pins, complete queue test config and
   observed environment per file. In parallel, failing files are re-run on the
   candidate. Failure attribution requires matching actual worker, platform,
   runtime, target interpreter bytes, declared toolchain, sealed environment and
   demand. A node that passes an equivalent rerun is a qualified flake; its
   source/worker/action evidence is recorded and an existing exact-node owner is
   reused before creating an issue.
5. Green: retain the tested candidate and publish it to `release-mq/<batch>-<uuid>`.
   Generate the vehicle ID once and save the branch before publication.
   Fresh queue states use separate vehicles even for identical candidates;
   recovery uses the saved branch.
   Open one vehicle PR listing every member, its approved SHA and `Refs #N`
   for its linked issues. Post the required context only on the candidate;
   member heads receive the informational `<context>/members` context.
   In `merge` mode, wait for `issue-link` success and merge only the vehicle
   with `gh pr merge --merge --match-head-commit <candidate>`. Strict branch
   protection supplies the final base check. If main moves, close the vehicle
   and requeue all members uncharged. Confirm each member is `MERGED` before
   recording completion. A moved member head closes the open vehicle and
   requests review; other members requeue uncharged. If a head moves during the
   final merge race, the vehicle remains a verified merge but that member gets
   a `member-head-moved` ledger record and the re-review comment. The batch
   ends as `merged-head-moved` once unchanged members are confirmed. It never
   claims the moved PR is MERGED or silently adopts its new head.
6. Red: bisect over batch prefixes, re-running only the newly failing files.
   The first failing prefix names the culprit, which gets `failure` with the
   node IDs and a comment. The other pull requests are re-queued at the front.

Missing or different observed environments return `attribution-inconclusive`.
The native recipe qualifier and environment parser use the collection owner's
selection codec, including compressed immutable node groups. Qualification binds
the original roots, assigned files, reported groups and exact recorder/pytest
command. File evidence retains the selected node IDs and each native action.
A grouped file qualifies only when every observed group has the same environment;
duplicate groups, changed group plans or a refused observation leave attribution
inconclusive. Population completeness remains the shard outcome owner's gate.
The failed outcomes remain in the batch record. The queue posts no status,
names no culprit, files no flake and requeues without charging member attempts.
The same rule applies to every bisection prefix. Old node-only cache entries
and historical failed actions without validated producer/interpreter evidence
cannot authorize attribution. A failed action still has no success receipt.
This source change needs separate reviewed runtime adoption; a merge alone
does not make historical records qualified.

An explicit dependency-pin or interpreter-placement refusal is instead
`runtime-blocked`: no identical inconclusive retries, PR attempt charge, flake,
code-failure blame or success status. The affected batch entries stay queued,
but the daemon skips them until an operator repairs the runtime and requests
fresh validation. Other queued entries remain runnable. The block's reason,
source commits/trees, full pins, selected path, config, reports, action keys and
logs survive restart in state and the batch record. A pre-submission refusal
has no fabricated action key. An interpreter whose presence is unknown may
still wait under PB's existing placement contract; unknown is not absence.

A report owns refusal classification: only its unobserved shards may identify
runtime refusal. Echoed refusal text from observed failing pytest outcomes is
ordinary test evidence, never a runtime blocker. Client-log fallback applies
only when no report exists. All concurrent results are recorded and inspected
for runtime refusals before classifying ordinary inconclusive results.

A shard with no pytest summary or outcome record and no explicit runtime refusal is inconclusive. Its files
are re-run up to `inconclusive_retries` times; if they stay unobserved the
batch posts nothing, re-queues its entries without charging attempts, and keeps
the existing backoff.

## Modes

| Mode | Statuses and comments | Merges |
|---|---|---|
| `dry-run` | logged as "would", never posted | never |
| `status` | posted; creates draft vehicles, then closes them and deletes their branches | never |
| `merge` | posted | yes; refuses to start unless `merge_enabled_file` exists |

## State

Everything lives under `state_dir`: `state.json` (queue, current batch,
history, flakes), `inbox/` (one file per `enqueue`, so only the daemon writes
the state), `ledger.jsonl` (every status, comment and merge, read before
posting again), `events.log` (one line per state change, and a heartbeat at
least every minute while a run is waiting), `STATUS.txt`, `batches/<id>/`
(the batch record and every `pbtest.py` JSON report, which holds each shard's
receipt path) and `baselines/<compatibility-digest>.json`. Old tree-only
baselines and history reports without matching compatibility identity are not
reused. Changing even an operational config field conservatively invalidates
evidence; the digest never aliases two full pins sharing a path abbreviation.
Version3 compatibility includes the actually launched pbtest path and rejects
older version2 evidence whose generation may have been resolved after execution.

Before a green verdict, restart requeues the interrupted entries with their
pins and attempt counts. After green, it resumes the same persisted candidate
and branch without another suite run. Vehicle lookup includes open, closed
and merged PRs, covering a crash after remote creation but before local receipt.
The ledger records the vehicle, candidate statuses and confirmed member merges.
Selection verifies that each member references an existing issue in this
repository, excluding pull request numbers. A missing issue reference refuses
the member before tests or publication.

Pending checks, transient GitHub refusals and delayed member state updates retain
the batch for the next daemon tick, up to `green_wait_s`, which defaults to
3,600 seconds from the persisted green decision. Restarts, crashes during
publication and changes in wait cause do not renew the deadline. A failed `issue-link` check ends the batch and
requeues its members with an attempt charge. Expired waits also release the
active batch. A confirmed vehicle merge never requeues integrated members.

Terminal disposition and any requeue are saved before remote cleanup. Cleanup
obligations live in `state.json` under `vehicle_cleanup`, outside the active
batch. They retry up to `max_attempts` with `stuck_backoff_s` between attempts.
Exhausted obligations stay visible for operator repair and do not hold later
batches. Status vehicles are drafts and close with branch deletion after the
verdict. A refused cleanup retains its obligation across restart.
Dry-run leaves retained cleanup obligations and their retry budgets unchanged.
Resume in status or merge mode to close the vehicles and delete their branches.
A repeated `once` reconciles a retained batch before accepting another batch
and refuses a mode change while it is active. A status refusal never permits
a merge.

## Configuration

The queue needs authenticated `gh` commands and authenticated Git pushes.
`Mirror.push_candidate` runs `git push` from the queue's bare mirror, so
`gh auth login` alone does not configure its HTTPS Git credentials.
For an operator-managed GitHub CLI identity, run `gh auth setup-git --hostname
github.com` as the user who runs the queue. Confirm that the user service can
access the same credential helper and identity before starting the shadow.
Managed agent launchers must provide both Git and `gh` capabilities. Keep
credentials out of queue configuration, unit files, PR bodies, and task comments.

The configured `pbtest` entrypoint is resolved once when configuration is
constructed, before discovery, module loading or submission. Its retained,
immutable generation path supplies discovery, outcome parsing, execution and
run/cache evidence for every candidate/base/rerun/retry/bisect phase. Replacing
the publisher symlink mid-run cannot change that binding. Restart/reconstruct
the queue configuration to deliberately adopt a newly published generation.

One JSON file per repository. `pbtest_args` must carry `--priority` and an
explicit `--timeout-s`, and may not set `--checkout`, `--python`, `--json` or
`--history`, which the queue owns.

```json
{
 "repo": "OWNER/NAME",
 "context": "pb-tests",
 "author": "OWNER",
 "base_branch": "main",
 "state_dir": "/home/USER/pbmergeq/NAME",
 "where": "HOST:~/pbmergeq/NAME/batches/{batch}",
 "pbtest": "/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py",
 "client_python": "/path/to/python-with-pytest",
 "test_python": "/path/to/the/target/interpreter",
 "pbtest_args": ["--tag", "x86", "--priority", "-10", "--timeout-s", "3600",
                 "--shards", "20", "--workers-per-shard", "4",
                 "--cpus-per-shard", "4", "--mem-gb", "8", "--max-clients", "0"],
 "test_paths": ["tests"],
 "skip_fleet_data_files": true,
 "batch_cap": 8,
 "merge_enabled_file": "/home/USER/pbmergeq/NAME/MERGE_MODE_GO",
 "tmpdir": "/home/USER/tmp"
}
```

`skip_fleet_data_files` leaves out files `pbtest.py` would refuse without a
`--data-manifest`, and each batch records which files it left out.

### Optional per-checkout interpreter policy (#1427)

Without `runtime_pins`, `test_python` remains a static string. For a repository
with independently provisioned reviewed environments, declare the owning
Python-literal pin sources and an absolute path template, for example:

```json
{
 "test_python": "/home/USER/venvs/pq-pb{pb:.8}-tessera-{ts:.8}/bin/python",
 "runtime_pins": {
  "pb": {"source": "prismaquant/prismabuild_runtime_contract.py", "name": "PRISMABUILD_DEV_PIN_COMMIT"},
  "ts": {"source": "prismaquant/tessera_runtime_contract.py", "name": "TESSERA_DEV_PIN_COMMIT"}
 }
}
```

Paths/names here are illustrative: configure the repository's actual pin owners.
Each source must stay inside its frozen checkout, including symlink resolution,
and hold exactly one top-level literal assignment of the named full lowercase
40-character Git commit. Missing, nonliteral, malformed or duplicate metadata
refuses deterministically. Templates permit only declared fields, optionally
`.1` through `.40` prefix precision; no attributes, indexing, conversions or
nested formatting. Every declared field must appear. Selection reads AST only;
it never imports the candidate or executes a checkout resolver on the coordinator.

Candidate, base, immediate rerun, inconclusive retries and bisected prefixes
select from their own checkouts. The full pins and pin-source byte digests are
recorded even when the path abbreviates them. The template is a selection hint,
not provenance authority: the published worker-side resolver and full installed
Git/RECORD/import-owner guard remain unchanged and authoritative. PB still owns
interpreter-path eligibility and placement. No environment is installed or patched.
Mutable installed metadata is not a signature or a safe runtime cache contract;
provision separate reviewed environments rather than changing one under users.

To retry blocked entries, stop this repository's daemon (the existing single-writer
lock applies), repair/provision the environment or policy, then run
`pbmergeq.py --config C resume-runtime b00012` and restart the daemon. Resume
only removes the named entry block; eligibility, fresh source/pin selection,
PB placement and the worker guard run again. A still-invalid runtime blocks
again without charging attempts. A manually requested `once` likewise validates
fresh sources; it never enqueues entries. A `runtime-blocked` once prints its
machine-readable verdict and exits 2. Other once verdicts retain their existing
exit 0 processing-completion semantics: read the verdict/statuses, not exit 0,
to decide whether tests were green. These are source contracts, not a
claim of live configuration or fleet activation.

## Commands

```bash
pbmergeq.py --config C enqueue PR SHA      # any process; prints the daemon's mode
pbmergeq.py --config C daemon --mode status  # run the queue
pbmergeq.py --config C once --mode dry-run PR [PR ...]  # one batch, outside the queue
pbmergeq.py --config C status                # print STATUS.txt
pbmergeq.py --config C resume-runtime BATCH   # request fresh validation; daemon stopped
```


### Duration hints across source changes (#1438)

Failure/baseline reuse retains the full current-tree runtime/config identity.
Historical file durations are placement advice only. The queue validates the
previous report's stored v3 identity against its own source tree and the current
runtime, full pins/pin-source bytes, configuration, domain and frozen pbtest path.
A completed existing v3 batch can supply its recorded source identity;
unidentified legacy reports remain ignored.

Git file modes/blobs must agree for every file/duration key in a reused shard.
Changed or missing files, including their mixed shard rows, use ordinary default
estimates. Changed collection plugins, pytest configuration or test-package
initializers invalidate the entire optional hint set. The derived duration-hints report retains only complete untouched
original rows, stdout and receipts; it grants no pass/fail/result/skip reuse and
does not alter the source report, test population, or sealed action. Missing
source objects or unusable history simply omit the optional advice. These are
historical estimates, not a speed measurement.

### Repeated failed candidate domains (#1450)

Removing only the culprit PR number does not remove its code from descendant
branches. After causal attribution confirms new failures, the queue retains a
negative record for the complete candidate tree, base tree, selected runtime,
full configuration and exact discovered test-file domain. Reconstructing that
same domain returns known-failure-blocked before any suite submission. The
original report, culprit and new node IDs remain attributable; the record
grants no passing result, skip reuse or success status.

A daemon hold retains only its existing Mirror-owned candidate worktree.
Before excluding any queued head, tick rechecks current GitHub eligibility and
full head, the clean owned checkout HEAD/tree, current fetched base tree,
runtime/configuration and discovery domain. Changed or unobserved meaning
releases the hold and uses ordinary fresh selection. Missing, dirty or
retargeted views cannot authorize suppression: they are retired through
Mirror, then rebuilt normally. An unowned or unobservable replacement is retained
for explicit recovery; only its suppression record is released. Views also retire when held entries leave or
are superseded. A corrected explicit head is eligible even when its ancestry
contains the original culprit commit. Immutable negative reports survive view
cleanup.

A supported once does not retain a queued hold or worktree. Its explicit
known-failure-blocked verdict exits 2, submits no repeated suite and creates no
success status. This is a source contract, not permission to activate a
daemon or change the fleet.


## PrismaBuild shadow on celestia

The checked-in `tools/fleet/mergeq/prismabuild.json` uses priority 0 for the
merge gate, ahead of priority -10 self-validation and measurement jobs. It
publishes all 20 shards with an explicit 3,600-second execution deadline.
Each shard declares 4 CPUs and 8 GiB with four pytest workers and one native
thread each, for at most 80 CPUs and 160 GiB across the full batch, with no
GPU demand. Portable work has no architecture tag. It runs the complete
`tests` inventory without fleet-data exclusions.

`tools/fleet/mergeq/pbmergeq-prismabuild.service` is a status-only user unit
with `Restart=always`. Its environment file is
`/home/rob/pbmergeq/prismabuild/shadow.env`, containing `PBMERGEQ_CLIENT`
and `PBMERGEQ_CONFIG` as absolute paths to the reviewed source entrypoint and
this config. The GitHub CLI must have managed repository access available to
that service. The config deliberately omits `merge_enabled_file`, so merge
mode refuses even if invoked separately. Installing this unit starts no fleet
worker, publishes no runtime, and changes no branch protection.

After installing the reviewed unit and its environment file, start only
`pbmergeq-prismabuild.service`. Do not touch the disabled PrismaQuant unit or
its state directory. Reviewers can continue manual merges during the shadow
period. `once --mode status PR PR` captures current heads for a shadow batch
and creates a draft vehicle without merging it. After recording the verdict,
the queue closes that vehicle and deletes its `release-mq/*` branch.
`once --mode merge` refuses
because that command carries no reviewed SHAs. Approved entries instead use
`enqueue PR SHA` and the daemon.

Retain a day's batch records and compare each verdict to manual merge outcomes
on the recorded base and member SHAs. Link candidate action keys, verified
receipts, vehicle URLs and any stale-base refusals. Source unit tests alone
establish neither live shadow operation nor the day of comparison.
After the shadow day passes, the Chief of Staff owns the reviewer switch
to `enqueue N SHA` and adds `pb-tests` to the required checks on `main`, as
authorized by Rob on 2026-10-25. Strict mode and `enforce_admins` stay enabled.
The Chief of Staff posts before/after protection evidence on PRI-1.
Merge mode remains disabled until separately authorized.
