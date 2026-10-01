# Batching merge queue (`pbmergeq.py`)

`tools/fleet/pbmergeq.py` tests a batch of pull requests with one full-suite
`pbtest.py` run and posts a commit status on each tested head (#1417). It
submits work only through the published `pbtest.py`; PrismaBuild keeps
sharding and placement.

## How a batch runs

1. Take up to `batch_cap` queued pull requests. Each must pass the
   eligibility filter: open, not a draft, a head branch in the repository
   itself (never a fork), opened by `author`, based on `base_branch`. An entry
   enqueued with a SHA is refused if the head has moved.
2. Fetch the base branch and each `refs/pull/N/head` into a bare mirror the
   queue owns, check that each fetched head equals what GitHub reports, and
   merge them in queue order onto a fresh worktree of the base. A pull request
   that conflicts with the base gets a comment and drops out; one that only
   conflicts with an earlier batch member is re-queued.
3. Run the suite once on the candidate with `pbtest.py --json`.
4. Judge by failure set. The verdict is the set difference of failing node
   IDs, candidate minus base. The base is run only on the files the candidate
   failed in, which is all the difference needs, and results are cached per
   tree SHA. In parallel, the failing files are re-run on the candidate: a
   node that passes there is a flake, recorded and filed once per node ID.
5. Green: post `success` on each included head. In `merge` mode, check that
   the base branch still points at the tested base, then merge in order with
   `gh pr merge --merge --match-head-commit <sha>`; if the base moved, the
   batch is re-queued and re-tested.
6. Red: bisect over batch prefixes, re-running only the newly failing files.
   The first failing prefix names the culprit, which gets `failure` with the
   node IDs and a comment. The other pull requests are re-queued at the front.

A shard with no pytest summary or outcome record is inconclusive. Its files
are re-run up to `inconclusive_retries` times; if they stay unobserved the
batch posts nothing, re-queues its entries and backs off.

## Modes

| Mode | Statuses and comments | Merges |
|---|---|---|
| `dry-run` | logged as "would", never posted | never |
| `status` | posted | never |
| `merge` | posted | yes; refuses to start unless `merge_enabled_file` exists |

## State

Everything lives under `state_dir`: `state.json` (queue, current batch,
history, flakes), `inbox/` (one file per `enqueue`, so only the daemon writes
the state), `ledger.jsonl` (every status, comment and merge, read before
posting again), `events.log` (one line per state change, and a heartbeat at
least every minute while a run is waiting), `STATUS.txt`, `batches/<id>/`
(the batch record and every `pbtest.py` JSON report, which holds each shard's
receipt path) and `baselines/<tree>.json`.

A restart re-queues the interrupted batch's entries at the front and runs it
again. The ledger keeps a status from being posted twice for the same batch,
and a merge is skipped when the ledger or GitHub says it already happened.

## Configuration

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

## Commands

```bash
pbmergeq.py --config C enqueue PR [SHA]      # any process; prints the daemon's mode
pbmergeq.py --config C daemon --mode status  # run the queue
pbmergeq.py --config C once --mode dry-run PR [PR ...]  # one batch, outside the queue
pbmergeq.py --config C status                # print STATUS.txt
```
