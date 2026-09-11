# Declaring an action's data, and warming it before the claim

Issue #487.  Two pieces: a second content-addressed input that says which
bytes an action will read off the shared mount, and a single loop on the
storage host that makes those bytes resident before anybody claims the action.

## Why

A campaign whose working set is far larger than the file server's ARC gets
nothing from caching.  Every byte is read once and never asked for again, so a
cache that fills behind the reader never hits.  The only read that can be made
fast is one that was already resident when the action started.

Measured on dl380g10 against the GLM-5.3-Flash census, 2026-09-11:

| arm | bytes | rate | wall |
|---|---|---|---|
| row already in ARC, read over NFS by 8 readers | 49.3 GB | 3298.7 MB/s | 14.94 s |
| never-read row, same shape, same readers | 49.3 GB | 306.9 MB/s | 160.77 s |

10.75x, 145.8 s saved on one row.  During the warm arm the four HDDs summed
8.6 MiB/s against 2.26 GB/s served -- 0.4% -- and ARC demand-data hits ran
2,318,508 against 3,721 misses.  The disks were never the constraint at any
reader count; HDD utilization never passed 88%.

## The input

`pbcampaign.data-manifest` is an ordinary `action.inputs` row
(`{id, sha256, bytes}`), so nothing in the action schema changes and the
existing CAS ingestion carries it.  The blob it addresses is a JSON manifest:

```json
{
  "schema": "prismaquant.prismabuild.data_manifest.v1",
  "produced_by": {"tool": "...", "commit": "...", "unix": 1789090615},
  "annotations": {"row_id": "row-0079"},
  "mount_prefix": "/mnt/shared",
  "entries": [
    {"path": "/mnt/shared/.../inputs/x.pt", "offset": 0,
     "bytes": 20973477, "sha256": null},
    {"path": "/mnt/shared/models/M/model-00096-of-00120.safetensors",
     "offset": 0, "bytes": 5369757696, "sha256": null}
  ],
  "entry_count": 868,
  "total_bytes": 63786036448
}
```

`validate_data_manifest` refuses anything that is not exact identity: a path
outside `mount_prefix`, a path that is not already normalized, a relative path,
a repeated `(path, offset)`, a zero-length entry, and totals that disagree with
the list.  `offset` is there because the consumer reads tensor byte ranges out
of safetensors shards rather than whole files, and a whole-file manifest would
price the warm at several times the bytes the action touches.

`entries` are in *consumption* order and are never re-sorted, so a warm cut
short by the budget leaves a useful prefix rather than a random subset.

`sha256` may be null on every entry, and is for the GLM census.  Hashing a
terabyte of calibration captures costs far more than the residency it buys,
and integrity is not what this contract carries.  What binds the byte list to
the action is that the *manifest file* is content-addressed like any other
input -- so the action key covers exactly these bytes, and a different list is
a different action.

**Attaching a manifest changes the action key.**  `inputs` is inside the
hashed action body (`core.py` `_normalize_action_body`), so the same command
with a manifest is a different action from the same command without one.
That is the property that makes the input safe -- no receipt can be answered
by a run that read different bytes -- but it does mean a campaign that gains
manifests gains new keys, and any `passes/` sidecars under the old keys are
orphaned.

Submitters:

- `pbrun.py --data-manifest PATH`
- `pbcampaign.py`: a per-row `data_manifest` field, which is just that flag.

`pbrun` also seals a summary into `params.data_manifest`
(`{input, mount_prefix, entry_count, total_bytes}`) so the prewarm loop's
headroom arithmetic can read a byte count per claimed action without fetching
and parsing a blob on every poll.

## The loop

`tools/fleet/prewarm_loop.py`, run by the supervisor on a box that declares
the `storage` role in `fleet_boxes.json`.  Every poll:

1. `ready_items()` -- the queue's own order, `(-priority, -passes, age)`.  The
   loop has no opinion about what runs next; it reads the same list the
   workers read.  **It is not a second dispatcher:** it never claims,
   reserves, reorders or writes an item.
2. The first `--lookahead` actions that carry a manifest.  Actions without one
   are passed over and do not consume the lookahead.
3. ARC headroom: `(c_max - size) * --arc-reserve-fraction`, minus the manifest
   bytes of every action claimed inside the last `--claim-grace-min` minutes.
   A manifest that does not fit is refused, and refusing writes nothing.
4. Reads each entry, in manifest order, through the host's local pool path
   (`--mount-map SHARED=LOCAL`), `--readers` at a time, `O_NOFOLLOW`, regular
   files only, paced by the pool's disks (below).
5. Writes `pb-queue/prewarm/<action_key>.json`.

### Pacing, and why the reader count is 1

The first deployment read with `--readers 8`, and the read rate was the only
thing it measured.  Measured against the rest of the fleet on 2026-09-11
(#499), that warm cost more than it saved: eight concurrent 1 MiB readers took
the four-spindle raidz1 to 73-83% utilization, 38-54 ms read await and
11 000-14 400 ms of backlog; the clients' sync writes queued behind that
backlog, their replies passed the NFS-over-RDMA timeout, the server's late
completions failed (`WC error: 10, remote access error`), and every in-flight
RPC on *every* client was retried after a reconnect.  Both Sparks' GPUs idled
100-130 s per row, against the ~3.5 min of prefetch the warm saved on one.
The same bytes read at 8-12% utilization and ~0-2 ms await cost nobody
anything.

So the loop is paced by what the disks are doing, not by a thread count.
Before every block it consults a sample of `/sys/block/<dev>/stat` for the
pool's own data vdev members -- discovered from `zpool status -P`
(`--pace-pool`), or named outright with `--disks` -- and holds while the worst
disk is over any of three caps:

| argument | default | harmless (measured) | stalling (measured) |
|---|---|---|---|
| `--max-util-pct` | 25 | 8-12 % | 73-83 % |
| `--max-read-await-ms` | 10 | 0-2 ms | 38-54 ms |
| `--max-backlog-ms` | 2000 | 300-450 ms | 11 000-14 400 ms |

The defaults are the shape that passed on the live fleet, not the midpoint
between the two measured states.  One reader at 40 % / 15 ms / 4 000 ms held
the disks to 31 % peak yet still dropped both Sparks' NFS clients to ~50
RPC/s for a minute (their own metrics collectors missed samples while it ran);
one reader at 25 % / 10 ms / 2 000 ms warmed a 63.8 GB row at 146.5 MB/s with
the clients untouched.  One reader is already enough to keep the pool busy:
98 % of that run's ARC misses were *prefetch* misses, so the size of each read
burst is set by ZFS's prefetcher (`zfetch_max_distance`), not by the reader,
and a second reader only adds queue depth the pacer then has to take back.

The three numbers are computed the way Netdata computes `disk_util`,
`disk_await` and `disk_backlog`, so the record and the chart an operator reads
afterwards are the same quantities.  `--pace-sample-s` bounds how often sysfs
is read (0.25 s); every block's check reads the cached verdict.  Because the
burst height belongs to the prefetcher, the sample interval is what bounds a
burst's *length*.

On the storage role, those samples are required evidence, not a best-effort
optimization.  Every data-vdev member discovered from the pool must provide a
readable stat row.  A per-block loss of one member or all rows holds before the
next read; a topology-discovery failure before a later cycle exits the role for
its supervisor to retry instead of creating an unpaced cycle.  The hold-start
event reports `telemetry_state` and `missing_devices`; the pacing report also
records `telemetry_gaps` beside the ordinary disk numbers.  After recovery the
first complete sample is only a new baseline: a second complete sample is
required before a read resumes.  The hold is interruptible through the
reader's stop event.  A direct fixture or non-storage caller that configures
no disks remains explicitly inactive (`disk_pacing.active: false`); that mode
is not a storage-role fallback.

Setting any cap to 0 disables that cap.  Setting all three off is how you
reproduce the pre-#499 behaviour, and it is not a supported production shape.

### The ARC arithmetic behind `--lookahead 1`

The lookahead is bounded by what the ARC can hold *besides* the rows that are
running, not by how far ahead the loop can see.  On dl380g10: `c_max` is
245 760 MiB (257.7 GB, and both spellings appear in the record: `arcstats`
counts bytes, Netdata's `zfs.arc_size` charts MiB), `--arc-reserve-fraction 0.8` leaves the loop 206.2 GB, and a GLM
census row is ~64 GB.  Two rows being read plus one warmed ahead is 191.4 GB,
14.8 GB under the budget.  A second lookahead row is 255.2 GB and does not
fit: the only way to warm it is to displace what a running row is still
reading, which is what the `arc_size` swing (205 -> 237 -> 220 GB) recorded on
2026-09-11.  The headroom check already refuses that row; `--lookahead 1`
means the loop does not spend a poll discovering it.

### What "never evicts a claimed row's data" means here

It is a budget rule, not a guarantee, and the code says so.  ZFS exposes no
way to pin a page, and the ARC target `c` is volatile on a shared box: it fell
99 GB inside one five-minute window on dl380g10 on 2026-09-11 without any
tenant asking for the memory.  What the loop guarantees is that it never
*asks* for more than the ceiling leaves after claimed actions are subtracted,
and it logs both headroom numbers (`c_max - size` and `c - size`) every cycle
so the gap between the budget and the outcome stays visible.

### The one write

`pb-queue/prewarm/<action_key>.json` is the only thing the loop writes,
anywhere.  It is a sidecar for the same reason `passes/` is one: the action is
still in `ready` when the warm runs, and rewriting a ready item races the claim
that may already have moved it -- which would resurrect a claimed action and
hand it to a second worker.  Nothing under the shared data mount is written.

### The receipt

`claim` copies the sidecar onto the claimed record as `prewarm`, in the write
it was already making, and `finish` carries it into the terminal record's
`detail.prewarm`.  A worker that measured its own residency wins: `finish`
uses `setdefault`.  An action nobody warmed carries no `prewarm` key at all,
because "nobody looked" and "warmed nothing" are different facts.

## Deploying

The `storage` role only exists once a runtime generation carrying
`prewarm_loop.py` is published.  A supervisor running an older generation
ignores the `roles` key; a supervisor running a newer one with no script logs
`role storage not startable` and keeps supervising its worker loops.  With no
manifest-carrying action in the queue the loop does nothing at all.
