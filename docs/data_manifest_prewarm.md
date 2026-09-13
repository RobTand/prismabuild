# Declaring an action's data, and warming it before the claim

Issue #487.  Two pieces: a second content-addressed input that says which
bytes an action will read off the shared mount, and a single loop on the
storage host that makes those bytes resident before anybody claims the action.

The [fleet storage host record](fleet_storage.md) describes the clients'
readahead proposal and DL380's existing sync/L2ARC/no-SLOG configuration
(#523). These host settings do not replace the prewarmer's disk pacing.

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

`annotations.phases` is optional and is what lets a manifest larger than the
cache be warmed at all.  It is a running byte sum over `entries`, in the order
the action reads them:

```json
"annotations": {
  "row_id": "joint-3c",
  "phases": [
    {"name": "head", "bytes": 14829322240, "cumulative_bytes": 14829322240},
    {"name": "layer-0", "bytes": 118380625920,
     "cumulative_bytes": 133209948160}
  ]
}
```

Names are unique and non-empty, `cumulative_bytes` never decreases, and the
last boundary is the manifest's `total_bytes`.  A table that breaks any of
those rules is treated as absent rather than repaired: windowing on the wrong
boundaries reads the wrong bytes and then records them as resident.  Every
other annotation stays uninterpreted -- PrismaBuild reads `row_id` for the
receipt's label and `phases` for the window, and nothing else.

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
2. The window of any claimed action that is reading a manifest larger than the
   budget moves forward first (below).  It does not consume the lookahead: a
   claimed action is not a row the lookahead is counting.
3. The first `--lookahead` actions the loop can still act on.  An action
   without a manifest, one below `--min-manifest-bytes`, one this loop has
   already warmed as far as the budget allows, and one that does not fit
   today's budget are each passed over and do not consume the lookahead: the
   window counts rows a warm could still make faster, and one oversized
   submission must not turn prewarm off for the rows behind it.  How much the
   loop holds resident ahead of the claim frontier is bounded by the budget,
   not by `--lookahead` -- step 4 subtracts every warmed, unclaimed row's
   bytes.
4. ARC headroom: `c_max * --arc-reserve-fraction`, minus what claimed actions
   may still read, and minus the bytes of every row this loop has already
   warmed that nobody has claimed yet (`warmed_reserve`).  A claim inside the
   last `--claim-grace-min` minutes reserves its whole manifest; a claim that
   reports progress reserves only the phases it has not read yet; a claim
   whose manifest was warmed a window at a time reserves that window, because
   bytes nobody warmed are on the spindles either way.  A manifest that fits
   no window at all is refused, and refusing writes nothing.
5. Reads the window's entries, in manifest order, through the host's local
   pool path (`--mount-map SHARED=LOCAL`), `--readers` at a time,
   `O_NOFOLLOW`, regular files only, paced by the pool's disks and by whether
   any client is reading (below).
6. Writes `pb-queue/prewarm/<action_key>.json`.

### Windows, when the manifest is larger than the ARC

The jobs that need prewarm most were the ones it refused.  The GLM joint pass
(chain step 3b `prepare`, 3c `run`) reads about 4.75 TB layer-major: 45 hidden
layers, layers 3-44 about 111-124 GB each, against an ARC `c_max` of 257.7 GB
and a 206 GB budget.  A whole-manifest `total > budget` check refuses that by
construction, so the pass read every byte off the spindles.

What has to fit in the cache was never the manifest.  It is the distance
between what the action has read and what the loop has made resident.  So when
`annotations.phases` is present the loop warms through the last phase boundary
whose `cumulative_bytes` fits the budget and records how far it got:

```json
{"status": "partial", "warmed_through_phase": "layer-1",
 "warmed_bytes": 133209948160, "window_start_bytes": 0,
 "bytes_warmed": 133209948160, "phased": true, "trigger": "claim"}
```

`warmed_bytes` is everything the loop has made resident for this manifest;
`bytes_warmed` is what the last read moved.  They differ only for a manifest
warmed a window at a time.  On later polls, when the action is claimed and its
progress record names a phase, the window advances from where it stopped and
the record is rewritten in place -- one file per key, so `already_warm` still
answers the same question.  The invariant the advance keeps is the budget's
own: warmed minus consumed never exceeds the budget, so the loop never holds
more of one manifest resident than the cache was going to keep anyway.

For a windowed row, "already warm" means *everything the budget allows is
resident*, which is the honest claim about a manifest that will never fit
whole.  A manifest without phases keeps the whole-manifest rule, unchanged.

### Following the running action, instead of its claim

#523 measured what the old trigger cost.  The loop warmed the next ready row
when the lookahead slot freed -- which is the instant the previous row was
claimed and started its own cold reads.  On 2026-09-12 the warm ran 4.3 s
ahead of the claim and overlapped that row's own reads at 256.7 MB/s: the
client saw no speedup at all, because the bytes arrived exactly as late as if
nobody had warmed them.

A claim is the end of the useful window, not the start of one.  An action that
declares `params.progress` writes `prismabuild.action_progress.v1` records to
`claimed/<key>.progress` (`PRISMABUILD_ACTION_PROGRESS_PATH`), which the loop
reads from the queue side without asking the worker anything.  The phases
before the one it names are read: the ARC may evict them, nothing is waiting
for them, and they leave the claimed reserve.  The next row becomes warmable
while the running one is still working.

Two rules keep that honest:

* The sealed request must declare the policy.  A record beside an action that
  asked for no progress reporting is not a report the platform asked for, and
  it does not move the budget.
* The record must be younger than the claim.  The launcher unlinks the path
  and mints a fresh token before every launch, and the token is deliberately
  not published to the queue, so a leftover record from an earlier attempt is
  rejected by its instant instead.

The cycle event says which signal fired, with the key, the phase and the bytes
it released:

```json
{"claimed_released_bytes": 14829322240,
 "progress_triggers": [{"action_key": "...", "phase": "layer-1",
                        "released_bytes": 14829322240}],
 "warmed": [{"action_key": "...", "trigger": "progress"}]}
```

`trigger` is an observation, not a label: a row is `"progress"` only when it
fits the budget with the release and would not have fit without it.  An action
that reports nothing keeps the claim-plus-`--claim-grace-min` behaviour it has
today.

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
disk is over a cap:

| argument | default | harmless (measured) | stalling (measured) | holds |
|---|---|---|---|---|
| `--client-active-mb-s` | 2.6 | 0 MB/s (nobody reading) | 26 MB/s and up | required |
| `--max-read-await-ms` | 10 | 0-2 ms | 38-54 ms | yes |
| `--max-backlog-ms` | 2000 | 300-450 ms | 11 000-14 400 ms | yes |
| `--max-util-pct` | 25 | 8-12 % | 73-83 % | no, recorded only |

### A hold needs a client to protect

The pacer exists to protect NFS clients, so the first question a hold answers
is whether anybody is reading.  Measured on dl380g10 at 2026-09-13 04:05Z: a
`zpool scrub` (53.4 % done, issuing 736 MB/s) drove sdb to 88 % utilization at
153.8 MB/s with 9.4 ms read await; the storage role's pacer had held for
5709 s cumulative and was holding at every sample, `util_pct` 100 and backlog
up to 26 s; and no NFS client read a byte for the whole window.  The loop
warmed nothing for hours and protected nobody.  Utilization answered "is the
disk busy"; nobody had asked that question.

So the verdict is an AND: hold while clients are reading **and** the pool is
over its service-time or backlog cap.  Client activity is read from this
host's own NFS server counters (`--nfsd-io`, the `io` line of
`/proc/net/rpc/nfsd`), which count bytes `nfsd` served; the loop reads the pool
through the host's local path, so its own reads never appear there and the
measurement cannot chase itself.  The threshold has to sit *below* the slowest
read worth protecting -- a client already slowed by the warm would otherwise
read as idle and release the hold that would give it the disks back -- so the
default is a tenth of the 26 MB/s cold-start client read #523 measured, an
order of magnitude above an idle mount's attribute traffic.

A host that cannot read the counter paces as though clients were always
reading: blind is not idle, and one missing file must not turn the pacer off
on the box that needs it most.  Missing *disk* telemetry still holds whoever
is reading, for the same reason it always did -- that hold is a blind pacer,
not a busy pool.  A hold is released at half the cap that started it, so a
disk sitting exactly on a threshold does not flap the reader once per sample.

`--max-util-pct` is still accepted and still recorded beside the rate, because
it is the number an operator compares against Netdata; it no longer holds on
its own.  Removing it would have broken every command line in
`fleet_boxes.json` and thrown away a measurement, for a flag whose only defect
was being consulted.

The pacing report separates the two questions an operator actually asks --
what pacing cost, and what it cost for nothing:

```json
{"held_seconds": 41.2, "held_seconds_total": 5709.0,
 "held_while_clients_active_s": 0.0, "held_while_clients_idle_s": 5709.0,
 "samples": 88, "samples_total": 21714,
 "clients_active": false, "client_read_mb_s": 0.0}
```

`held_seconds` prices one row and resets with it; the totals belong to the
role and outlive the pacer each cycle rebuilds.

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

The disk verdict and its prior stat baseline remain shared across sequential
rows in a poll, but receipt accounting is row-scoped: samples, means, maxima,
holds, held seconds, and telemetry gaps are reset at each row boundary.  An
existing hold is never reset by that accounting boundary.

Setting any cap to 0 disables that cap.  Setting the await and backlog caps
off is how you reproduce the pre-#499 behaviour, and it is not a supported
production shape.  Setting `--client-active-mb-s 0` holds whenever the disks
are over, whoever is reading, which is the pre-#523 shape.

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
means the loop does not spend a poll discovering it while the head of the
queue is still cold.  Once the head is warm the loop advances past it in the
same poll, and it is then the headroom check that decides whether a second row
fits -- a refusal that reads no data and writes no record.

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
`detail.prewarm`.  For a windowed manifest that copy is the claim-time
snapshot: the window advances after the claim, so the sidecar under
`pb-queue/prewarm/` is where the final window state lives.  A worker that measured its own residency wins: `finish`
uses `setdefault`.  An action nobody warmed carries no `prewarm` key at all,
because "nobody looked" and "warmed nothing" are different facts.

## Deploying

### Restarting the role after a publication

The supervisor spawns the storage role from `_current_root()` -- the generation
`/mnt/shared/prismabuild-fleet/repo` points at -- but a role that is already
running keeps executing the file it started with.  Publishing a new generation
therefore does not move a running loop.  Restart it by ending that one process
and letting the supervisor respawn it:

```
pgrep -af prewarm_loop.py            # the role's pid and its arguments
kill -TERM <role pid>                # the loop, never the supervisor
```

The supervisor respawns the role from the live generation within its poll
interval.  Verify with the `spawned`/`role` line in the supervisor log and the
new pid's `/proc/<pid>/cmdline`, which must name the new generation directory.

Never `systemctl stop prismabuild-supervisor.service` for this: that stops
every worker loop on the box as well, and the role is the only thing that
needed to move.

Measured example (2026-09-13, dl380g10): `kill -TERM 2486293` at 04:12:24Z
ended the role running generation `953c95e5fb52`; at 04:12:29Z the supervisor,
already re-exec'd to `176021ec3efb-1789272169-6533ecef8514`, spawned pid
2201912 with the same arguments from
`runtime-generations/176021ec3efb-1789272169-6533ecef8514/tools/prewarm_loop.py`.
Its first cycles logged `ready=1`, `pacing_active=true`, `arc_size` 251.2 GB
of a 257.7 GB `c_max`.

The `storage` role only exists once a runtime generation carrying
`prewarm_loop.py` is published.  A supervisor running an older generation
ignores the `roles` key; a supervisor running a newer one with no script logs
`role storage not startable` and keeps supervising its worker loops.  With no
manifest-carrying action in the queue the loop does nothing at all.
